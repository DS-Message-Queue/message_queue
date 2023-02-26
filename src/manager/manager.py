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

class ManagerService(pb2_grpc.ManagerServiceServicer):

    def RegisterBroker(self, broker, context):
        print('register broker called')
        response = requests.get(url='http://127.0.0.1:8002/broker', params={'host': broker.host, 'port': broker.port})
        response = response.json()

        status = False
        if response['status'] == 'Success':
            status = True
        return pb2.Status(status = status, brokerId = response['brokerId'])
    
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
    
    def GetUpdates(self, request, context):
        queries = ['insert', 'update', 'delete']
        for q in queries:
            yield pb2.Query(query=q)


class Manager:
    def __init__(self, name):
        
        # HTTP Endpoint
        t = multiprocessing.Process(target=self.serve_endpoint)
        t.start()

        # accept registrations from brokers in a different process
        server_thread = multiprocessing.Process(target=self.serve_grpc)
        server_thread.start()

        t.join()
        server_thread.join()
    
    def serve_endpoint(self):
        MyServer(__name__)

    def serve_grpc(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        pb2_grpc.add_ManagerServiceServicer_to_server(ManagerService(), server)
        server.add_insecure_port('[::]:50051')
        print('manager listening at:', 'localhost:50051')
        server.start()
        server.wait_for_termination()
