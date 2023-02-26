import grpc
import json
import asyncio
from concurrent import futures
import time
from src.manager.manager import ManagerService
import src.protos.managerservice_pb2_grpc as m_pb2_grpc
import src.protos.managerservice_pb2 as m_pb2
from src.HTTPServer.HTTPServer import MyServer
import multiprocessing

class ManagerConnection:
    """
    Client for gRPC functionality
    """

    def __init__(self, host, port, token):

        self.token = token
        self.registered = False

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(host, port))

        # bind the client and the server
        self.stub = m_pb2_grpc.ManagerServiceStub(self.channel)

    def health_check(self):
        """
            If manager connection is active, this return True
            Else this connects to the manager (blocking call) and then returns False
        """
        printed = False
        ret = True
        while True:
            try:
                self.stub.HealthCheck(m_pb2.HeartBeat())
            except:
                ret = False
                if not printed:
                    print('Manager disconnected, retrying...')
                    printed = True
                continue
            if printed:
                print('Manager connected.')
            return ret

    def register_replica_if_required(self):
        needs_register = True
        if self.registered:  # if already registered once
            needs_register = not self.health_check()

        if needs_register:
            self.health_check()
            Response = self.stub.RegisterReplica(m_pb2.ReplicaDetails(
                token=self.token
            ))

    def get_updates(self):
        """
            Get updates from the manager
        """
        while True:
            try:
                Queries = self.stub.GetUpdates(m_pb2.Request())
                for q in Queries:
                    print(q.query)
                break
            except:
                self.register_replica_if_required()
                continue


class ManagerReplicaService(m_pb2_grpc.ManagerServiceServicer):

    def SendTransaction(self, transaction_req, context):
        transaction = json.loads(transaction_req.data)
        response = self.process_transaction(transaction)
        return m_pb2.TransactionResponse(data=json.dumps(response).encode('utf-8'))
    
    def process_transaction(self, transaction):
        print(transaction)
        response = {'status': 'Success', 'Message': 'Successfully executed!'}
        return response


class ManagerReplica:
    def __init__(self, name, http_host, http_port, grpc_host, grpc_port):

        # HTTP Endpoint
        t = multiprocessing.Process(target=self.serve_endpoint, args=(
            name, http_host, http_port, grpc_host, grpc_port
        ))
        t.start()

        with open('./src/manager_replica/replica.json', 'r') as config_file:
            self.config = json.load(config_file)

        manager = ManagerConnection(
            self.config['server_host'], 
            self.config['server_port'], 
            self.config['token']
        )

        # register replica at the manager
        manager.register_replica_if_required()

        # get updates from the manager
        manager.get_updates()

        # start grpc server for replica
        server_thread = multiprocessing.Process(target=self.serve_grpc)
        server_thread.start()

        t.join()
        server_thread.join()

    def serve_endpoint(self, name, http_host, http_port, grpc_host, grpc_port):
        MyServer(name, http_host, http_port, grpc_host, grpc_port)

    def serve_grpc(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        m_pb2_grpc.add_ManagerServiceServicer_to_server(ManagerReplicaService(), server)
        server.add_insecure_port('[::]:50053')
        print('manager replica listening at:', 'localhost:50053')
        server.start()
        server.wait_for_termination()
