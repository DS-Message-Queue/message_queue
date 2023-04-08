import grpc
import json
import multiprocessing
from concurrent import futures
import src.protos.managerservice_pb2_grpc as pb2_grpc
import src.protos.managerservice_pb2 as pb2
import src.protos.brokerservice_pb2_grpc as b_pb2_grpc
import src.protos.brokerservice_pb2 as b_pb2
from src.HTTPServer.HTTPServer import MyServer
import src.Database.main_db as db
import threading
from src.Healthchecker.healthchecker import HealthChecker
from datetime import datetime
import time
from src.WAL.WAL import WriteAheadLog
from src.WAL.recovery import CrashRecovery

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

    def get_updates(self, topics):
        for topic in topics:
            for partition in topics[topic]:
                if partition == 'producers' or partition == 'consumers':
                    continue
                Queries = self.stub.GetUpdates(b_pb2.Request(
                    topic = topic, partition = str(partition)
                ))
                for q in Queries:
                    yield q.query
                

    def send_transaction(self, transaction):
        Response = self.stub.SendTransaction(b_pb2.Transaction(
            data=bytes(json.dumps(transaction).encode('utf-8'))
        ))
        response = json.loads(Response.data)
        return response
    

class SelfManagerConnection:
    """
    Client for gRPC functionality
    """
    def __init__(self, server_host, server_port):
        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(server_host, server_port))
        # bind the client and the server
        self.stub = pb2_grpc.ManagerServiceStub(self.channel)

    def send_transaction(self, transaction):
        self.stub.ReceiveUpdatesFromBroker(pb2.UpdatesFromBroker())


class ManagerService(pb2_grpc.ManagerServiceServicer):

    def __init__(self) -> None:
        super().__init__()
        self.total_brokers_connected = 0
        self.last_picked_broker = 0
        self.__lock = threading.Lock()
        self.brokers = {}
        self.__db = db.databases()
        self.__topics = {}
        self.__consumers = {}
        self.__producers = {}
        self.__health_checker = HealthChecker()
        self.brokers_connected = []
        self.raft_ports = {}
        self.replicas = {}
        self.last_inactive_broker = 1
        self.__topics, self.__producers, self.__consumers = self.__db.recover_from_crash(
            self.__topics, self.__producers, self.__consumers)
        
        self.wal = WriteAheadLog()
        
        # Perform WAL Recovery
        self.__queries = CrashRecovery().recoverLogs("query")
        if len(self.__queries) == 0:
            self.wal.clearlogfile()

    def connect_to_broker(self, host, port, raftport):
        self.total_brokers_connected += 1
        broker = 1 + len(self.brokers_connected)
        if broker not in self.raft_ports:
            self.raft_ports[broker] = raftport
        # store the connection in the broker
        self.brokers[broker] = BrokerConnection(host, port)

        # broker in now connected
        self.brokers_connected.append(broker)

        print('broker ' + str(broker) + ' connected.')
        return broker

    def RegisterBroker(self, broker, context):
        print('register broker called')
        broker_id = self.connect_to_broker(broker.host, broker.port, broker.raft_port)
        try:
            self.__health_checker.insert_into_broker(broker_id,str(datetime.now()))
        except:
            pass
        transaction = {
            "req": "Init",
            "topics": self.__topics,
            "producers": self.__producers
        }

        self.brokers[broker_id].send_transaction(transaction)
        
        return pb2.Status(status=True, brokerId=broker_id)

    def HealthCheck(self, heartbeat, context):
        return heartbeat

    def RegisterReplica(self, replica_details, context):
        print('manager replica register requested')
        print('manager replica connected.')
        # print('replica token:', replica_details.token)

        # if token is valid:
        return pb2.Response(
            status=True,
            replicaId=1   # future work
        )

    # Might be useful in Replica
    def PushUpdates(self, query_iter, context):
        for q in query_iter:
            print(q.query)
        # connect to db and execute the queries here
        return pb2.Response()

    def GetUpdates(self, request, context):
        self.ReceiveUpdatesFromBroker(pb2.UpdatesFromBroker(), context)
        queries = self.__queries[:]
        self.__queries.clear()
        for q in queries:
            # wal end
            self.wal.logSuccess(q[0], "query")
            yield pb2.Query(query=q[1])

    def ReceiveUpdatesFromBroker(self,req, context):
        for broker in self.brokers:
            res = None
            try:
                res = self.brokers[broker].get_updates(self.__topics)
            except Exception as e:
                print('reveiveUpdates Exception:', e)
                pass
            try:
                self.__health_checker.insert_into_broker(broker,str(datetime.now()))
            except:
                pass
            if res is not None:
                for query in res:
                    # WAL start
                    self.__db.run_query(query)
                    # WAL end

                    # wal log start here and end when query is sent to replica
                    txn_id = self.wal.logEvent("query", query)
                    self.__queries.append((txn_id, query))
        return pb2.UpdatesFromBroker()

    def SendTransaction(self, transaction_req, context):
        transaction = json.loads(transaction_req.data)
        output = {}
        transaction_type = transaction['req']

        if transaction_type == 'GetTopics':
            output = {"status": "success", "topics": self.__topics}
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'GetPartition':
            topic_requested = transaction['topic']
            if topic_requested not in self.__topics:
                output = {"status": "failure", "message": "Invalid Topic."}
                return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))
            
            partitions = []
            for each_partition in self.__topics[topic_requested]:
                partitions.append(each_partition)
            output = {"status": "success", "partitions": partitions,
                      "number_of_partitions": len(partitions)}
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'ClearDatabase':
            isLockAvailable = self.__lock.acquire(blocking=False)
            if isLockAvailable is False:
                output = {"status": "failure",
                          "message": "Lock cannot be acquired."}
                return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))
            try:
                self.__db.clear_database()
                self.__topics = {}
                self.__consumers = {}
                self.__producers = {}
                self.wal.clearlogfile()
                output = {"status": "success",
                          "message": "Database Cleared successfully."}
            except:
                output = {"status": "failure",
                          "message": "Couldn't clear database."}
            self.__lock.release()
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'CreateTopic':
            isLockAvailable = self.__lock.acquire(blocking=False)
            if isLockAvailable is False:
                output = {"status": "failure",
                          "message": "Lock cannot be acquired."}
                return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))
            
            topic_requested = transaction['topic']
            if topic_requested in self.__topics:
                output = {"status": "failure",
                          "message": "Topic already exists."}
                self.__lock.release()
                return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))
        
            else:
                try:
                    # START THE WAL LOGGING
                    # Setting partition id default to 1
                    # txn_id = self.wal.logEvent(broker, "Create Topic", topic_requested)
                    print(self.brokers)
                    for broker in self.brokers:
                        self.brokers[broker].send_transaction(transaction)
                        if broker not in self.replicas:
                            self.replicas[broker] = [broker, (broker%self.total_brokers_connected)+1, ((broker+1)%self.total_brokers_connected)+1]
                        transaction_to_broker = {'req': "ReplicaHandle", "replica_list": [(topic_requested, str(broker)), (topic_requested, str((broker%self.total_brokers_connected)+1)), (topic_requested, str(((broker+1)%self.total_brokers_connected)+1))], "other_raftports": [self.raft_ports[(broker%self.total_brokers_connected)+1], self.raft_ports[((broker+1)%self.total_brokers_connected)+1]]}
                        self.brokers[broker].send_transaction(transaction_to_broker)
                        
                        print(self.replicas)
                        try:
                            self.__health_checker.insert_into_broker(broker,str(datetime.now()))
                        except:
                            pass
                    output_query = self.__db.insert_topic(
                        topic_requested, 1, 0)
                    self.__topics[topic_requested] = {1: {"messages": []}}
                    output = {"status": "success",
                              "message": "Topic created."}
                    
                    # success for this log event will be written when the
                    # query is sent to replica
                    txn_id = self.wal.logEvent("query", output_query)
                    self.__queries.append((txn_id, output_query))

                    # self.wal.logSuccess(txn_id, broker, "Create Topic", topic_requested)
                    # END WAL TRANSACTION
                except Exception as e:
                    output = {"status": "failure",
                              "message": "Topic creation failed."}
                    print('exectpiton in create: ', e)
                self.__lock.release()
                return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'ProducerRegister':
            topic_requested = transaction['topic']
            isLockAvailable = self.__lock.acquire(blocking=False)
            if isLockAvailable is False:
                output = {"status": "failure",
                          "message": "Lock cannot be acquired."}
                return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))
            if topic_requested not in self.__topics:
                output = {}
                try:
                    # Setting partition id default to 1
                    # START THE WAL LOGGING
                    # txn_id = self.wal.logEvent(broker, "Create Topic", topic_requested)
                    output_query = self.__db.insert_topic(
                        topic_requested, 1, 0)
                    self.__topics[topic_requested] = {1: {"messages": []}}
                    for broker in self.brokers:
                        input = {'req': "CreateTopic", "topic": topic_requested}
                        self.brokers[broker].send_transaction(input)
                        try:
                            self.__health_checker.insert_into_broker(broker, str(datetime.now()))
                        except:
                            pass

                    # The wal end for this query will be set when it is send to replica
                    txn_id = self.wal.logEvent("query", output_query)
                    self.__queries.append((txn_id, output_query))

                    # self.wal.logSuccess(txn_id, broker, "Create Topic", topic_requested)
                    # END THE WAL LOGGING
                except:
                    output = {"status": "failure",
                              "message": "Producer Registration Failed."}
            if len(output) > 0 and output["status"] == "failure":
                self.__lock.release()
                return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))
            try:
                # START THE WAL LOGGING
                # txn_id = self.wal.logEvent(broker, "Register Producer", len(self.__producers) + 1, topic_requested)
                for each_partition in self.__topics[topic_requested]:
                    if each_partition == 'consumers' or each_partition == 'producers':
                        continue
                    output_query = self.__db.insert_for_producer(
                        len(self.__producers) + 1, topic_requested, each_partition)
                for broker in self.brokers:
                    input = {'req': transaction_type, "topic": topic_requested, "producer_id": len(
                            self.__producers) + 1}
                    self.brokers[broker].send_transaction(input)
                    try:
                        self.__health_checker.insert_into_broker(broker,str(datetime.now()))
                    except:
                        pass

                self.__producers[len(self.__producers) + 1] = {'topic': topic_requested}
                output = {"status": "success",
                          "message": "Producer created successfully.", "producer_id": len(self.__producers)}
                try:
                    self.__health_checker.insert_into_producer(len(self.__producers), str(datetime.now()))
                except:
                    pass
                
                # self.wal.logSuccess(txn_id, broker, "Register Producer", len(self.__producers) + 1, topic_requested)
                # END THE WAL LOGGING
            except Exception as e:
                print('exception:', e)
                output = {"status": "failure",
                          "message": "Producer Registration Failed."}
            self.__lock.release()
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'Enqueue':
            for _i in range(self.total_brokers_connected):
                broker = self.pick_broker()
                if broker == 0:
                    output = {"status": "failure",
                                "message": "No brokers to handle request."}
                    break
                    # return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))
                else:
                    try:
                        # START THE WAL LOGGING
                        # txn_id = self.wal.logEvent(broker, "Enqueue", len(self.__producers) + 1, topic_requested, transaction['message'])
                        print("Called for broker",broker)
                        output = self.brokers[broker].send_transaction(transaction)
                        try:
                            self.__health_checker.insert_into_broker(broker,str(datetime.now()))
                            self.__health_checker.insert_into_producer(transaction['producer_id'],str(datetime.now()))
                        except:
                            pass
                        # self.wal.logSuccess(txn_id, broker, "Enqueue", len(self.__producers) + 1, topic_requested, transaction['message'])
                        break
                        # END THE WAL LOGGING
                    except Exception as e:
                        print(e,"Here error")
                        self.brokers.pop(broker, None)
                        # output = {"status": "failure",
                        #         "message": "Could not publish message"}
                        # continue
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'EnqueueWithPartition':
            broker = transaction['partition']
            if broker not in self.brokers:
                output = {"status": "failure",
                            "message": "partition does not exist."}
                # return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))
            else:
                try:
                    # START THE WAL LOGGING
                    # txn_id = self.wal.logEvent(broker, "Enqueue", len(self.__producers) + 1, topic_requested, transaction['message'])
                    print("Called for broker",broker)
                    output = self.brokers[broker].send_transaction(transaction)
                    try:
                        self.__health_checker.insert_into_broker(broker,str(datetime.now()))
                        self.__health_checker.insert_into_producer(transaction['producer_id'],str(datetime.now()))
                    except:
                        pass
                    # self.wal.logSuccess(txn_id, broker, "Enqueue", len(self.__producers) + 1, topic_requested, transaction['message'])
                    # END THE WAL LOGGING
                except Exception as e:
                    print(e,"Here error")
                    self.brokers.pop(broker, None)
                    # output = {"status": "failure",
                    #         "message": "Could not publish message"}
                    # continue
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        else:
            output = {"status": "failure", "message": "Invalid Operation"}
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

    def pick_broker(self):
        self.__lock.acquire(blocking=True)
        nextPick = ((self.last_picked_broker) %
                    self.total_brokers_connected) + 1
        i = 0
        while i < self.total_brokers_connected and nextPick not in self.brokers:
            nextPick = ((nextPick) % self.total_brokers_connected) + 1
            i += 1
        if i == self.total_brokers_connected:
            self.last_picked_broker = 0
            self.__lock.release()
            return 0
        else:
            self.last_picked_broker = nextPick
            self.__lock.release()
            return nextPick


# TODO: Need to come up with a way to get updates from each broker after every 5 seconds.
class Manager:
    def __init__(self, name, http_host, http_port, grpc_host, grpc_port):
        self.__grpc_host =  grpc_host
        self.__grpc_port = grpc_port

        # accept registrations from brokers in a different process
        server_thread = multiprocessing.Process(target=self.serve_grpc)
        server_thread.start()

        time.sleep(5)
        self_thread = multiprocessing.Process(target=self.call_brokers)
        self_thread.start()

        # HTTP Endpoint
        self.serve_endpoint(name, http_host, http_port, grpc_host, grpc_port)

        server_thread.join()
        self_thread.join()
    
    def call_brokers(self):
        own_manager_rpc = SelfManagerConnection(self.__grpc_host, self.__grpc_port)
        while(1):
            own_manager_rpc.send_transaction({})
            time.sleep(5)

    def serve_endpoint(self, name, http_host, http_port, grpc_host, grpc_port):
        MyServer(name, http_host, http_port, grpc_host, grpc_port)

    def serve_grpc(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        pb2_grpc.add_ManagerServiceServicer_to_server(ManagerService(), server)
        server.add_insecure_port('[::]:50051')
        print('manager listening at:', 'localhost:50051')
        server.start()
        server.wait_for_termination()
