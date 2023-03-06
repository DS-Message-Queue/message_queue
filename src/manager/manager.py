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

    def get_updates(self):
        Queries = self.stub.GetUpdates(b_pb2.Request())
        for q in Queries.queries:
            print(q)
        # return Queries.queries

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
        self.__queries = []
        self.brokers_connected = []
        self.last_inactive_broker = 1
        self.__topics, self.__producers, self.__consumers = self.__db.recover_from_crash(
            self.__topics, self.__producers, self.__consumers)
        # Perform WAL Recovery

    def connect_to_broker(self, host, port):
        self.total_brokers_connected += 1
        broker = 1 + len(self.brokers_connected)

        # store the connection in the broker
        self.brokers[broker] = BrokerConnection(host, port)

        # broker in now connected
        self.brokers_connected.append(broker)

        print('broker ' + str(broker) + ' connected.')
        return broker

    def RegisterBroker(self, broker, context):
        print('register broker called')
        broker_id = self.connect_to_broker(broker.host, broker.port)
        try:
            self.__health_checker.insert_into_broker(broker_id,datetime.now())
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
        for q in self.__queries:
            yield pb2.Query(query=q)

    def ReceiveUpdatesFromBroker(self,req, context):
        transaction = {'req' : 'Poll'}
        for broker in self.brokers:
            res = self.brokers[broker].send_transaction(transaction)
            for query in res['queries']:
                self.__db.run_query(query)
        return pb2.UpdatesFromBroker()

    def SendTransaction(self, transaction_req, context):
        transaction = json.loads(transaction_req.data)
        output = {}
        transaction_type = transaction['req']

        if transaction_type == 'GetTopics':
            output = {"status": "success", "topics": self.__topics}

        elif transaction_type == 'GetPartition':
            topic_requested = transaction['topic']
            if topic_requested not in self.__topics:
                output = {"status": "failure", "message": "Invalid Topic."}
            partitions = []
            for each_partition in self.__topics[topic_requested]:
                partitions.append(each_partition)
            output = {"status": "success", "partitions": partitions,
                      "number_of_partitions": len(partitions)}

        elif transaction_type == 'ClearDatabase':
            isLockAvailable = self.__lock.acquire(blocking=False)
            if isLockAvailable is False:
                output = {"status": "failure",
                          "message": "Lock cannot be acquired."}
            try:
                self.__db.clear_database()
                output = {"status": "success",
                          "message": "Database Cleared successfully."}
            except:
                output = {"status": "failure",
                          "message": "Couldn't clear database."}

        elif transaction_type == 'CreateTopic':
            isLockAvailable = self.__lock.acquire(blocking=False)
            if isLockAvailable is False:
                output = {"status": "failure",
                          "message": "Lock cannot be acquired."}
            topic_requested = transaction['topic']
            if topic_requested in self.__topics:
                output = {"status": "failure",
                          "message": "Topic already exists."}
            else:
                try:
                    # START THE WAL LOGGING
                    # Setting partition id default to 1
                    output_query = self.__db.insert_topic(
                        topic_requested, 1, 0)
                    self.__topics[topic_requested] = {1: {"messages": []}}
                    for broker in self.brokers:
                        self.brokers[broker].send_transaction(transaction)
                        try:
                            self.__health_checker.insert_into_broker(broker,datetime.now())
                        except:
                            pass
                    output = {"status": "success",
                              "message": "Topic created."}
                    self.__queries.append(output_query)
                    # END WAL TRANSACTION
                except:
                    output = {"status": "failure",
                              "message": "Topic creation failed."}

        elif transaction_type == 'ProducerRegister':
            topic_requested = transaction['topic']
            isLockAvailable = self.__lock.acquire(blocking=False)
            if isLockAvailable is False:
                output = {"status": "failure",
                          "message": "Lock cannot be acquired."}
            if topic_requested not in self.__topics:
                try:
                    # Setting partition id default to 1
                    # START THE WAL LOGGING
                    output_query = self.__db.insert_topic(
                        topic_requested, 1, 0)
                    self.__topics[topic_requested] = {1: {"messages": []}}
                    for broker in self.brokers:
                        input = {'req': transaction_type, "topic": topic_requested, "producer_id": len(
                            self.__producers) + 1}
                        self.brokers[broker].send_transaction(input)
                        try:
                            self.__health_checker.insert_into_broker(broker,datetime.now())
                        except:
                            pass

                    self.__queries.append(output_query)
                    # END THE WAL LOGGING
                except:
                    output = {"status": "failure",
                              "message": "Producer Registration Failed."}
            try:
                # START THE WAL LOGGING
                temp_queries = []
                for each_partition in self.__topics[topic_requested]:
                    output_query = self.__db.insert_for_producer(
                        len(self.__producers) + 1, topic_requested, each_partition)
                    temp_queries.append(output_query)
                for broker in self.brokers:
                    self.brokers[broker].send_transaction(transaction)
                    try:
                        self.__health_checker.insert_into_broker(broker,datetime.now())
                    except:
                        pass

                self.__producers[len(self.__producers) +
                                 1]["topic"] = topic_requested
                output = {"status": "success",
                          "message": "Producer created successfully.", "producer_id": len(self.__producers)}
                self.__queries = (self.__queries + temp_queries).copy()
                try:
                    self.__health_checker.insert_into_producer(len(self.__producers) +1,datetime.now())
                except:
                    pass
                # END THE WAL LOGGING
            except:
                output = {"status": "failure",
                          "message": "Producer Registration Failed."}

        elif transaction_type == 'Enqueue':

            for _i in range(self.total_brokers_connected):
                broker = self.pick_broker(self)
                if broker == 0:
                    output = {"status": "failure",
                              "message": "No brokers to handle request."}
                else:
                    try:
                        # START THE WAL LOGGING
                        output = self.brokers[broker].send_transaction(transaction)
                        try:
                            self.__health_checker.insert_into_broker(broker,datetime.now())
                            self.__health_checker.insert_into_producer(transaction['producer_id'],datetime.now())
                        except:
                            pass
                        break
                        # END THE WAL LOGGING
                    except:
                        self.brokers.pop(broker, None)
                        continue

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
        # HTTP Endpoint
        t = multiprocessing.Process(target=self.serve_endpoint, args=(
            name, http_host, http_port, grpc_host, grpc_port
        ))
        t.start()

        # accept registrations from brokers in a different process
        server_thread = multiprocessing.Process(target=self.serve_grpc)
        server_thread.start()

        time.sleep(2)
        self_thread = multiprocessing.Process(target=self.call_brokers)
        self_thread.start()

        t.join()
        server_thread.join()
        self_thread.join()
    
    def call_brokers(self):
        print("This gets called")
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
