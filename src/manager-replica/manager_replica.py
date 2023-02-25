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

    def register_replica(self, token):
        Queries = self.stub.RegisterReplica(m_pb2.ReplicaDetails(
            token=token
        ))
        for q in Queries:
            print(q.query)

    def get_local_updates(self):
        queries = ['insert', 'update', 'delete']
        for q in queries:
            yield m_pb2.Query(query=q)

    def push_updates(self):
        self.stub.PushUpdates(self.get_local_updates())

    def get_updates(self):
        Queries = self.stub.GetUpdates(m_pb2.Request())
        for q in Queries:
            print(q.query)


class ManagerReplica:
    def __init__(self):
        with open('replica.json', 'r') as config_file:
            self.config = json.load(config_file)

        manager = ManagerConnection(self.config['server_host'], self.config['server_port'])

        manager.health_check()

        # register replica at the manager
        manager.register_replica(self.config['token'])

        # push local updates to the manager
        manager.push_updates()

        # get updated from the manager
        manager.get_updates()

        # define HTTP Endpoints here


if __name__ == '__main__':
    ManagerReplica()
