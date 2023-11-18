'''Message Queue'''
import json
from queue import Queue
from datetime import datetime
import threading
import time
import grpc
import src.Database.main_db as db
import src.protos.managerservice_pb2_grpc as r_pb2_grpc
import src.protos.managerservice_pb2 as r_pb2
import src.protos.brokerservice_pb2_grpc as b_pb2_grpc
import src.protos.brokerservice_pb2 as b_pb2
from src.WAL.WAL import WriteAheadLog
from src.WAL.recovery import CrashRecovery
from src.Healthchecker.healthchecker import HealthChecker


class ReplicaConnection:
    """
    Client for gRPC functionality
    """

    def __init__(self, server_host, server_port):

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(server_host, server_port))

        # bind the client and the server
        self.stub = r_pb2_grpc.ManagerServiceStub(self.channel)

    def push_updates(self, query_iter):
        self.stub.PushUpdates(query_iter)


class BrokerConnection:
    """
    Client for gRPC functionality
    """

    def __init__(self, host, port):

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(host, port))

        # bind the client and the server
        self.stub = b_pb2_grpc.BrokerServiceStub(self.channel)

    def get_updates(self, topic, partition):
        """
            Get updates the corresponding from broker
        """
        queries = self.stub.GetUpdates(b_pb2.Request(
            topic=topic, partition=partition
        ))
        for q in queries:
            yield q.query

    def send_transaction(self, transaction):

        Response = self.stub.SendTransaction(b_pb2.Transaction(
            data=bytes(json.dumps(transaction).encode('utf-8'))
        ))

        response = json.loads(Response.data)
        return response


class MessageQueue:
    def __init__(self, mq: Queue, bq: Queue, rq: Queue) -> None:

        # shared objects
        self.mq = mq
        self.bq = bq
        self.rq = rq

        # replicated objects
        self.topics = {}
        self.producers = {}

        # database connections
        self.db = db.databases()
        self.health_checker = HealthChecker()

        # manager objects'
        self.replica_connected = False
        self.wal = WriteAheadLog()
        self.last_picked_broker = 0
        self.__lock = threading.Lock()
        self.brokers = {}
        self.__health_checker = HealthChecker()
        self.brokers_connected = []
        self.raft_ports = {}
        self.broker_partitions = {}

        # manager replica connection
        self.replica = ReplicaConnection('localhost', '50053')

        # WAL recovery
        self.queries = CrashRecovery().recoverLogs("query")  # Perform WAL Recovery
        if len(self.queries) == 0:
            self.wal.clearlogfile()

    def __get_new_broker_id(self):
        if len(self.brokers_connected) == 0:
            return 1
        # see if there exists an unused id
        max_broker_id = max(self.brokers_connected)
        for id in range(1, max_broker_id):
            if id not in self.brokers_connected:
                return id
        return len(self.brokers_connected) + 1

    def connect_to_broker(self, host, port, raftport):
        # set the first unused id
        broker = self.__get_new_broker_id()
        if broker not in self.raft_ports:
            self.raft_ports[broker] = raftport
        # store the connection
        self.brokers[broker] = BrokerConnection(host, port)

        # broker in now connected
        self.brokers_connected.append(broker)

        print('broker ' + str(broker) + ' connected.')
        return broker

    def send_replica_handle(self, broker, topic_requested):
        n = len(self.brokers)
        raftportstosend = []  # partition networks the broker is in

        # broker b owns partition b
        # broker b has replications of partitions b + 1 and b + 2
        topic_partitions = [
            (topic_requested, str(broker)),                # b
            (topic_requested, str(broker % n + 1)),        # b + 1
            (topic_requested, str((broker + 1) % n + 1))   # b + 2
        ]
        if broker not in self.broker_partitions:
            self.broker_partitions[broker] = []
        self.broker_partitions[broker].extend(topic_partitions)

        # broker b owned partition has replications in brokers b - 1 and b - 2
        # hence broker b's partition network has brokers b, b - 1 and b - 2
        # broker b is present in the partition networks of  b, b + 1 and b + 2
        for _, partition in topic_partitions:
            broker_p = int(partition)
            raftportstosend.append([
                self.raft_ports[broker_p],                         # b
                self.raft_ports[(broker_p + n - 1 - 1) % n + 1],   # b - 1
                self.raft_ports[(broker_p + n - 1 - 2) % n + 1],   # b - 2
            ])

        # send only other brokers in partition networks
        for partition in raftportstosend:
            partition.remove(self.raft_ports[broker])

        transaction_to_broker = {
            'req': 'ReplicaHandle',
            'topic_partitions': topic_partitions,
            'other_raftports': raftportstosend}
        self.brokers[broker].send_transaction(transaction_to_broker)

    def pick_broker(self, n):
        self.__lock.acquire(blocking=True)
        nextPick = ((self.last_picked_broker) %
                    n) + 1
        i = 0
        while i < n and nextPick not in self.brokers:
            nextPick = ((nextPick) % n) + 1
            i += 1
        if i == n:
            self.last_picked_broker = 0
            self.__lock.release()
            return 0
        else:
            self.last_picked_broker = nextPick
            self.__lock.release()
            return nextPick

    def create_topic(self, transaction):
        topic_requested = transaction["topic"]
        try:
            for broker_id, broker in self.brokers.items():
                broker.send_transaction(transaction)
                self.send_replica_handle(broker_id, topic_requested)
                try:
                    self.health_checker.insert_into_broker(
                        broker_id, str(datetime.now()))
                except Exception:
                    pass
            # Setting partition id default to 1
            output_query = self.db.insert_topic(
                topic_requested, 1, 0)
            self.topics[topic_requested] = {1: {"messages": []}}
            # success for this log event will be written when the query is sent to replica
            txn_id = self.wal.logEvent("query", output_query)
            self.queries.append((txn_id, output_query))
        except Exception as e:
            print('exception in create: ', e)

    def process_transaction(self, transaction):
        transaction_type = transaction['req']

        if transaction_type == 'ClearDatabase':
            try:
                self.db.clear_database()
                self.topics = {}
                self.producers = {}
                self.wal.clearlogfile()
            except Exception as e:
                print('ClearDatabase exception:', e)

        elif transaction_type == 'CreateTopic':
            self.create_topic(transaction)

        elif transaction_type == 'ProducerRegister':
            topic_requested = transaction['topic']
            producer_id = transaction['producer_id']
            if topic_requested not in self.topics:
                self.create_topic(transaction)
            try:
                for each_partition in self.topics[topic_requested]:
                    if each_partition == 'consumers' or each_partition == 'producers':
                        continue
                    output_query = self.db.insert_for_producer(
                        len(self.producers) + 1, topic_requested, each_partition)
                    # no need to send producer register query to replica
                for broker in self.brokers:
                    input = {"req": transaction_type,
                             "topic": topic_requested, "producer_id": producer_id}
                    self.brokers[broker].send_transaction(input)
                    try:
                        self.health_checker.insert_into_broker(
                            broker, str(datetime.now()))
                    except:
                        pass
                self.producers[producer_id] = {'topic': topic_requested}
                try:
                    self.health_checker.insert_into_producer(
                        producer_id, str(datetime.now()))
                except:
                    pass
            except Exception as e:
                print('ProducerRegister exception:', e)

        elif transaction_type == 'Enqueue':
            n = len(self.brokers)
            for _i in range(n):
                broker = self.pick_broker(n)
                if broker == 0:
                    print('Fatal: No brokers to handle request.')
                    break
                else:
                    try:
                        # print("Called for broker", broker)
                        output = self.brokers[broker].send_transaction(
                            transaction)
                        try:
                            self.health_checker.insert_into_broker(
                                broker, str(datetime.now()))
                            self.health_checker.insert_into_producer(
                                transaction['producer_id'], str(datetime.now()))
                        except Exception:
                            pass
                        break
                    except Exception as e:
                        print(e)
                        print("broker", broker, "disconnected.")
                        self.brokers.pop(broker, None)
                        if broker in self.brokers_connected:
                            self.brokers_connected.remove(broker)

        elif transaction_type == 'EnqueueWithPartition':
            broker = transaction['partition']
            if broker not in self.brokers:
                print('Fatal: No broker to handle request.')
            else:
                try:
                    # print("Called for broker", broker)
                    output = self.brokers[broker].send_transaction(transaction)
                    try:
                        self.health_checker.insert_into_broker(
                            broker, str(datetime.now()))
                        self.health_checker.insert_into_producer(
                            transaction['producer_id'], str(datetime.now()))
                    except Exception:
                        pass
                except Exception as e:
                    print(e, "Here error")
                    self.brokers.pop(broker, None)
                    if broker in self.brokers_connected:
                        self.brokers_connected.remove(broker)

        else:
            print('Invalid transaction:', transaction)

    def handle_replica_connection(self, replica):
        # wait for the replica to start its server
        time.sleep(3)
        self.replica_connected = True
        print('replica connected:', replica)

    def handle_broker_connection(self, broker):
        broker_id = self.connect_to_broker(
            broker.host, broker.port, broker.raft_port)
        try:
            self.__health_checker.insert_into_broker(
                broker_id, str(datetime.now()))
        except Exception:
            pass

        transaction = {
            "broker_id": broker_id,
            "req": "Init",
            "topics": self.topics,
            "producers": self.producers,
        }

        self.brokers[broker_id].send_transaction(transaction)

        # make sure the broker gets get all the topics
        if len(self.brokers_connected) >= 4:
            for broker in self.brokers_connected:
                for topic in self.topics:
                    self.send_replica_handle(broker, topic)

    def get_updates(self):
        '''
            gets updates from brokers
        '''
        for q in self.queries:
            yield r_pb2.Query(query=q[1])

    def receive_updates_from_brokers(self):
        visited = set()
        brokers = [b for b in self.brokers]
        for broker in brokers:
            res = None
            try:
                if broker not in self.broker_partitions:
                    continue
                for topic_partition in self.broker_partitions[broker]:
                    if topic_partition in visited:
                        continue
                    res = self.brokers[broker].get_updates(
                        topic_partition[0], topic_partition[1])
                    
                    # process result
                    if res is not None:
                        for query in res:
                            self.db.run_query(query)
                            txn_id = self.wal.logEvent("query", query)
                            self.queries.append((txn_id, query))

                    visited.add(topic_partition)
                try:
                    self.__health_checker.insert_into_broker(
                        broker, str(datetime.now()))
                except:
                    pass
            except Exception as e:
                print('ReceiveUpdatesFromBroker exception:', e)
                print('broker', broker, 'disconnected.')
                self.brokers.pop(broker, None)
                if broker in self.brokers_connected:
                    self.brokers_connected.remove(broker)

    def worker(self):
        print('Message Queue processing')
        while True:
            # check for manager replica connection changes
            try:
                while True:
                    replica = self.rq.get_nowait()
                    self.handle_replica_connection(replica)
            except:
                # all changes handled
                pass

            # check for broker connection changes
            try:
                while True:
                    broker = self.bq.get_nowait()
                    self.handle_broker_connection(broker)
            except:
                # all changes handled
                pass

            # check for pending transactions
            try:
                transaction = self.mq.get_nowait()
                self.process_transaction(transaction)
            except:
                # no pending transactions
                pass

            # get updates from brokers
            self.receive_updates_from_brokers()

            # push updates to manager replica
            if self.replica_connected and len(self.queries) > 0:
                try:
                    self.replica.push_updates(self.get_updates())
                    for q in self.queries:
                        self.wal.logSuccess(q[0], "query")
                    self.queries.clear()
                except Exception as e:
                    self.replica_connected = False
                    print('push updates exception:', e)
