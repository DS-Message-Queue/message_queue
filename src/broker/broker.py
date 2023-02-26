import grpc
import json
import asyncio
from concurrent import futures
import time
import src.protos.managerservice_pb2_grpc as m_pb2_grpc
import src.protos.managerservice_pb2 as m_pb2
import src.protos.brokerservice_pb2_grpc as b_pb2_grpc
import src.protos.brokerservice_pb2 as b_pb2
import multiprocessing


class ManagerConnection:
    """
    Client for gRPC functionality
    """

    def __init__(self, server_host, server_port, broker_host, broker_port):

        # instantiate broker_id
        self.broker_id = None
        
        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(server_host, server_port))

        # bind the client and the server
        self.stub = m_pb2_grpc.ManagerServiceStub(self.channel)

        
        # broker server communication channel
        self.broker_channel = grpc.insecure_channel(
            '{}:{}'.format(broker_host, broker_port))

        # bind to broker server
        self.broker_stub = b_pb2_grpc.BrokerServiceStub(self.broker_channel)

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

    def register_broker_if_required(self, host, port, token):
        """
            Checks if manager is down and if it is, then re-register once it is up
        """
        needs_register = True
        if self.broker_id != None:  # if already registered once
            needs_register = not self.health_check()

        if needs_register:
            self.health_check()
            Status = self.stub.RegisterBroker(m_pb2.BrokerDetails(
                host=host, port=port, token=token
            ))

            if Status.status:
                print('Successfully registered.')
                self.broker_id = Status.brokerId
                self.broker_stub.ResetBroker(b_pb2.BrokerDetails(
                    brokerId = Status.brokerId
                ))


class BrokerService(b_pb2_grpc.BrokerServiceServicer):

    def __init__(self):
        super().__init__()
        self.broker_id = None

    def clear_data(self):
        pass

    def ResetBroker(self, broker_details, context):
        self.broker_id = broker_details.brokerId
        self.clear_data()
        return b_pb2.Status()

    def GetUpdates(self, request, context):
        # Send data from here to Manager
        return b_pb2.Queries(
            queries=['insert', 'delete', 'update']
        )

    def SendTransaction(self, transaction_req, context):
        transaction = json.loads(transaction_req.data)

        # process the transaction
        self.process_transaction(transaction)

        response = {'status': 'success', 'message': 'successfully added!'}
        return b_pb2.Response(data=bytes(json.dumps(response).encode('utf-8')))

    def process_transaction(self, transaction):
        print(transaction)


class Broker:
    def __init__(self):

        # retrieve broker config
        with open('./src/broker/broker.json', 'r') as config_file:
            self.config = json.load(config_file)
            self.host = self.config['host']
            self.port = self.config['port']
            self.token = self.config['token']

        # start broker service
        t = multiprocessing.Process(target=self.serve)
        t.start()

        # manager connection
        client = ManagerConnection(
            self.config['server_host'], self.config['server_port'],
            self.config['host'], self.config['port']
        )

        while True:
            client.register_broker_if_required(self.host, self.port, self.token)
            time.sleep(1)

        t.join()

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        b_pb2_grpc.add_BrokerServiceServicer_to_server(BrokerService(), server)
        ip = '{}:{}'.format(self.config['host'], self.config['port'])
        server.add_insecure_port('[::]:' + self.config['port'])
        print('broker listening at:', ip)
        server.start()
        server.wait_for_termination()
