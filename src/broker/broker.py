import grpc
import json
import asyncio
from concurrent import futures
import time
import managerservice_pb2_grpc as m_pb2_grpc
import managerservice_pb2 as m_pb2
import brokerservice_pb2_grpc as b_pb2_grpc
import brokerservice_pb2 as b_pb2
import threading

class ManagerConnection:
    """
    Client for gRPC functionality
    """

    def __init__(self, host, port):

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(host, port))

        # bind the client and the server
        self.stub = m_pb2_grpc.ManagerServiceStub(self.channel)

    def health_check(self):
        printed = False
        while True:
            try:
                self.stub.HealthCheck(m_pb2.HeartBeat())
            except:
                if not printed:
                    print('Manager disconnected, retrying...')
                continue
            if printed:
                print('Manager connected.')
            break

    def register_broker(self, host, port, token):
        Status = self.stub.RegisterBroker(m_pb2.BrokerDetails(
            host=host, port=port, token=token
        ))

        if Status.status:
            print('Successfully registered.')


class BrokerService(b_pb2_grpc.BrokerServiceServicer):

    def GetUpdates(self, request, context):
        return b_pb2.Queries(
            queries = ['insert', 'delete', 'update']
        )
    
    def SendTransaction(self, transaction_req, context):
        transaction = json.loads(transaction_req.data)
        
        #process the transaction
        self.process_transaction(transaction)
        
        response = {'status': 'success', 'message': 'successfully added!'}
        return b_pb2.Response(data = bytes(json.dumps(response).encode('utf-8')))
    
    def process_transaction(self, transaction):
        print(transaction)


class Broker:
    def __init__(self):
        with open('broker.json', 'r') as config_file:
            self.config = json.load(config_file)

        t = threading.Thread(target=self.serve)
        t.start()

        client = ManagerConnection(self.config['server_host'], self.config['server_port'])

        client.health_check()

        host = self.config['host']
        port = self.config['port']
        token = self.config['token']
        client.register_broker(host, port, token)

        t.join()

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        b_pb2_grpc.add_BrokerServiceServicer_to_server(BrokerService(), server)
        ip = '{}:{}'.format(self.config['host'], self.config['port'])
        server.add_insecure_port('[::]:' + self.config['port'])
        print('broker listening at:', ip)
        server.start()
        server.wait_for_termination()


if __name__ == '__main__':
    Broker()
