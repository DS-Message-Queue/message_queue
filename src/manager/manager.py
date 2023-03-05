import grpc
import json
import asyncio
import time
import requests
import multiprocessing
from concurrent import futures
import src.protos.managerservice_pb2_grpc as pb2_grpc
import src.protos.managerservice_pb2 as pb2
import src.protos.brokerservice_pb2_grpc as b_pb2_grpc
import src.protos.brokerservice_pb2 as b_pb2
from src.HTTPServer.HTTPServer import MyServer
import src.Database.main_db as db

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
        #return Queries.queries

    def send_transaction(self, transaction):
        Response = self.stub.SendTransaction(b_pb2.Transaction(
            data = bytes(json.dumps(transaction).encode('utf-8'))
        ))
        response = json.loads(Response.data)
        return response

class ManagerService(pb2_grpc.ManagerServiceServicer):

    def __init__(self) -> None:
        super().__init__()
        self.brokers_connected = []
        self.brokers = {}
        
        self.__db = db.databases()
        self.__db.clear_database()

    def connect_to_broker(self, host, port):
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
        return pb2.Status(status = True, brokerId = broker_id)
    
    def HealthCheck(self, heartbeat, context):
        return heartbeat
    
    def RegisterReplica(self, replica_details, context):
        print('manager replica register requested')
        print('manager replica connected.')
        # print('replica token:', replica_details.token)

        # if token is valid:
        return pb2.Response(
            status = True,
            replicaId = 1   # future work
        )
            
    def PushUpdates(self, query_iter, context):
        for q in query_iter:
            print(q.query)
        # connect to db and execute the queries here
        return pb2.Response()
    
    def GetUpdates(self, request, context):
        # needs sync with HTTPServer to get the queries
        cnt = 1
        i = 1
        queries = []
        for t in ["T-1", "T-2", "T-3"]:
                for j in [1,2,3]:
                    queries.append(self.__db.insert_topic(t, j))
                    queries.append(self.__db.insert_for_producer(i, t, j))
                    #queries.append(self.__db.insert_for_consumer(i, t, j))
                    queries.append(self.__db.insert_for_messages(t, "Meesagex - " + str(cnt), 5, j))
                    cnt += 1
                i += 1
        
        for q in queries:
            yield pb2.Query(query=q)

    def SendTransaction(self, transaction_req, context):
        # load balance
        broker = 1
        transaction = json.loads(transaction_req.data)
        response = self.brokers[broker].send_transaction(transaction)
        return pb2.TransactionResponse(data=json.dumps(response).encode('utf-8'))


class Manager:
    def __init__(self, name, http_host, http_port, grpc_host, grpc_port):
        
        # HTTP Endpoint
        t = multiprocessing.Process(target=self.serve_endpoint, args=(
            name, http_host, http_port, grpc_host, grpc_port
        ))
        t.start()

        # accept registrations from brokers in a different process
        server_thread = multiprocessing.Process(target=self.serve_grpc)
        server_thread.start()

        t.join()
        server_thread.join()
    
    def serve_endpoint(self, name, http_host, http_port, grpc_host, grpc_port):
        MyServer(name, http_host, http_port, grpc_host, grpc_port)

    def serve_grpc(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        pb2_grpc.add_ManagerServiceServicer_to_server(ManagerService(), server)
        server.add_insecure_port('[::]:50051')
        print('manager listening at:', 'localhost:50051')
        server.start()
        server.wait_for_termination()
