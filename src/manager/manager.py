import grpc
import json
import asyncio
from concurrent import futures
import time
import managerservice_pb2_grpc as pb2_grpc
import managerservice_pb2 as pb2
import brokerservice_pb2_grpc as b_pb2_grpc
import brokerservice_pb2 as b_pb2
import threading

# maybe we need to lock these variables
brokers = {}
brokers_connected = []

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
        print(response)
        #return response


class ManagerService(pb2_grpc.ManagerServiceServicer):

    def connect_to_broker(self, host, port):
        # get broker id
        broker = 1 + len(brokers_connected)

        # store the connection in the broker
        brokers[broker] = BrokerConnection(host, port)
        
        # broker in now connected
        brokers_connected.append(broker)

        print('broker ' + str(broker) + ' connected.')

    def RegisterBroker(self, broker, context):

        # print(broker.host, broker.port, broker.token)

        self.connect_to_broker(broker.host, broker.port)

        return pb2.Status(
            status = True
        )
    
    def HealthCheck(self, heartbeat, context):
        return heartbeat
    
    def RegisterReplica(self, replica_details, context):
        print('replica token:', replica_details.token)
        queries = ['insert', 'update', 'delete']
        for q in queries:
            yield pb2.Query(query=q)
            
    def PushUpdates(self, query_iter, context):
        for q in query_iter:
            print(q.query)
        return pb2.Response()
    
    def GetUpdates(seld, request, context):
        queries = ['insert', 'update', 'delete']
        for q in queries:
            yield pb2.Query(query=q)


class Manager:
    def __init__(self):
        # accept registrations from brokers in a different thread
        server_thread = threading.Thread(target=self.serve)
        server_thread.start()

        while (len(brokers_connected) == 0):
            pass
        time.sleep(1)

        # At least 1 broker is now connected
        
        # Provide the HTTP Endpoint here and send transactions to brokers as following:

        brokers[1].get_updates()

        transaction = {'req': 'Enqueue', 'pid': 12, 'topic': 'foo', 'partition': 'B', 'message': 'test'}
        brokers[1].send_transaction(transaction)

        server_thread.join()

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        pb2_grpc.add_ManagerServiceServicer_to_server(ManagerService(), server)
        server.add_insecure_port('[::]:50051')
        print('manager listening at:', 'localhost:50051')
        server.start()
        server.wait_for_termination()


if __name__ == '__main__':
    Manager()
