from flask import Flask, request
from src.controller.main import Message_Queue
import src.protos.managerservice_pb2_grpc as pb2_grpc
import src.protos.managerservice_pb2 as pb2
import grpc
import json


# message_queue = Message_Queue()

class ManagerConnection:
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
        Response = self.stub.SendTransaction(pb2.Transaction(
            data=bytes(json.dumps(transaction).encode('utf-8'))
        ))
        response = json.loads(Response.data)
        # print(response)
        return response



class MyServerHandler:
    def __init__(self, name, grpc_host, grpc_port):
        # Connect to db server here and store necessary variables in self
        # Be aware that this gets called everytime an HTTP request is made
        # So maybe there is a better place for it

        self.manager_rpc = ManagerConnection(grpc_host, grpc_port)
        self.app = Flask(name)

        #  Returns the list of topics
        @self.app.route('/topics')
        def __get_topics():
            return self.get_topics()

        #  Returns the list of topics
        @self.app.route('/topics/partition')
        def __get_partition():
            return self.get_partition()

        @self.app.route('/consumer/consume')
        def __dequeue():
            return self.dequeue()

        @self.app.route('/size')
        def __size():
            return self.size()

        @self.app.route('/cleardb')
        def __clear_db():
            return self.clear_db()

        @self.app.route('/topics', methods=['POST'])
        def __create_topic():
            return self.create_topic()

        @self.app.route('/consumer/register', methods=['POST'])
        def __consumer_register():
            return self.consumer_register()

        @self.app.route('/producer/register', methods=['POST'])
        def __producer_register():
            return self.producer_register()

        @self.app.route('/producer/produce', methods=['POST'])
        def __enqueue():
            return self.enqueue()

    def run(self, host, port):
        self.app.run(host=host, port=port)
        # from waitress import serve
        # serve(self.app, host=host, port=port)

    # def __del__(self):
    #    # close connection to db here

    def get_topics(self):
        # ListTopics
        print('topics requested')
        response = {}
        transaction = {'req': 'GetTopics'}
        ret = self.manager_rpc.send_transaction(transaction)
        status = 400
        response.update(ret)
        if response['status'] == 'success':
            status = 200
        response = json.dumps(response)
        return response, status

    def get_partition(self):
        # List Partitions for topic
        print('partitions requested')
        json_is_valid = True
        data_json = dict(request.args)
        response = {}
        status = 400
        if len(data_json) == 0:
            print('invalid data in params')
            response['status'] = 'failure'
            response['message'] = 'invalid data in params'
            json_is_valid = False

        if json_is_valid and len(data_json) == 1 and 'topic' in data_json:
            topic = data_json['topic']
            # generate consumer_id
            transaction = {'req': 'GetPartition', 'topic': topic}
            ret = self.manager_rpc.send_transaction(transaction)
            response.update(ret)
            if response['status'] == 'success':
                status = 200
        else:
            # only accept one topic in data_json
            response['status'] = 'failure'
            response['message'] = 'invalid data in params'
        response = json.dumps(response)
        return response, status

    def dequeue(self):
        # dequeue
        print("dequeue requested")
        json_is_valid = True
        data_json = dict(request.args)
        response = {}
        status = 400
        if len(data_json) == 0:
            print('invalid data in params')
            response['status'] = 'failure'
            response['message'] = 'invalid data in params'
            json_is_valid = False

        if json_is_valid and len(data_json) == 3 and 'topic' in data_json \
                                                    and 'consumer_id' in data_json:
            topic        =  data_json['topic']
            consumer_id  =  int(data_json['consumer_id'])
            partition = int(data_json['partition'])

            # dequeue
            transaction = {'req': 'DequeueWithPartition', 'consumer_id': consumer_id, 'topic': topic, 'partition': partition}
            ret = self.manager_rpc.send_transaction(transaction)

            response.update(ret)

            if response['status'] == 'success':
                status = 200

        elif json_is_valid and len(data_json) == 2 and 'topic' in data_json \
                                                    and 'consumer_id' in data_json:
            topic        =  data_json['topic']
            consumer_id  =  int(data_json['consumer_id'])

            # dequeue
            transaction = {'req': 'Dequeue', 'consumer_id': consumer_id, 'topic': topic}
            ret = self.manager_rpc.send_transaction(transaction)

            response.update(ret)

            if response['status'] == 'success':
                status = 200

            else:
                print(response['message'])

        else:
            print('invalid data in parameters')
            # incorrect params in data_json
            response['status'] = 'failure'
            response['message'] = 'invalid data in params'

        response = json.dumps(response)
        return response, status

    def size(self):
        # size
        print("size requested")
        json_is_valid = True
        data_json = dict(request.args)
        response = {}
        status = 400
        if len(data_json) == 0:
            print('invalid params given')
            json_is_valid = False

        if json_is_valid and len(data_json) == 2 and 'topic' in data_json \
                                                    and 'consumer_id' in data_json:
            topic        =  data_json['topic']
            consumer_id  =  int(data_json['consumer_id'])

            # size
            # ret = message_queue.log_size(topic,consumer_id)
            transaction = {'req': 'Size', 'topic': topic, 'consumer_id': consumer_id}
            ret = self.manager_rpc.send_transaction(transaction)
            response.update(ret)
            if response['status'] == 'success':
                status = 200

        else:
            # incorrect params in data_json
            response['status'] = 'failure'
            response['message'] = 'invalid data in params'

        response = json.dumps(response)
        return response, status

    def clear_db(self):
        print('clear requested')
        params = dict(request.args)
        response = {}
        status = 400
        if 'code' in params and params['code'] == 'xBjfq12nh':
            transaction = {'req': 'ClearDatabase'}
            self.manager_rpc.send_transaction(transaction)
            response['status'] = 'success'
            status = 200
        else:
            response['status'] = 'failure'
        response = json.dumps(response)
        return response, status

    def create_topic(self):
        # CreateTopic
        print('create topic requested')
        data = request.data
        response = {}
        status = 400
        json_is_valid = True
        data_json = {}
        try:
            data_json = json.loads(data)
        except json.decoder.JSONDecodeError:
            print('JSON decode failed')
            json_is_valid = False
        if json_is_valid and len(data_json) == 1 and 'topic_name' in data_json:
            topic = data_json['topic_name']
            transaction = {'req': 'CreateTopic', 'topic': topic}
            ret = self.manager_rpc.send_transaction(transaction)
            response.update(ret)
            if response['status'] == 'success':
                status = 200
        else:
            # only accept topic_name in data_json
            response['status'] = 'failure'
            response['message'] = 'invalid data in params'

        response = json.dumps(response)
        return response, status

    def consumer_register(self):
        # consumer register
        print("consumer register requested")

        data = request.data
        response = {}
        status = 400
        json_is_valid = True
        data_json = {}
        try:
            data_json = json.loads(data)
        except json.decoder.JSONDecodeError:
            print('JSON decode failed')
            json_is_valid = False

        if json_is_valid and len(data_json) == 1 and 'topic' in data_json:
            topic = data_json['topic']

            # generate consumer_id
            # ret = message_queue.register_consumer(topic)
            transaction = {'req': 'ConsumerRegister', 'topic': topic}
            ret = self.manager_rpc.send_transaction(transaction)
            response.update(ret)
            if response['status'] == 'success':
                status = 200

        else:
            # only accept one topic in data_json
            response['status'] = 'failure'
            response['message'] = 'invalid data in params'

        response = json.dumps(response)
        return response, status

    def producer_register(self):
        # producer register
        print("producer register requested")
        data = request.data
        response = {}
        status = 400
        json_is_valid = True
        data_json = {}
        try:
            data_json = json.loads(data)
        except json.decoder.JSONDecodeError:
            print('JSON decode failed')
            json_is_valid = False

        if json_is_valid and len(data_json) == 1 and 'topic' in data_json:
            topic = data_json['topic']
            # generate producer_id
            transaction = {'req': 'ProducerRegister', 'topic': topic}
            ret = self.manager_rpc.send_transaction(transaction)
            response.update(ret)
            if response['status'] == 'success':
                status = 200
        else:
            # only accept one topic in data_json
            response['status'] = 'failure'
            response['message'] = 'invalid data in params'
        response = json.dumps(response)
        return response, status

    def enqueue(self):
        # enqueue
        print("produce requested")
        data = request.data
        response = {}
        status = 400
        json_is_valid = True
        data_json = {}
        try:
            data_json = json.loads(data)
        except json.decoder.JSONDecodeError:
            print('JSON decode failed')
            json_is_valid = False

        if json_is_valid and len(data_json) == 3 and 'topic' in data_json \
                and 'producer_id' in data_json \
                and 'message' in data_json:
            topic = data_json['topic']
            producer_id = data_json['producer_id']
            message = data_json['message']
            # enqueue
            transaction = {'req': 'Enqueue', 'topic': topic,
                           "producer_id": producer_id, "message": message}
            ret = self.manager_rpc.send_transaction(transaction)
            response.update(ret)
            
            if 'status' not in response:
                # incorrect params in data_json
                print('invalid response received from rpc server:', response)
                response['status'] = 'failure'
                response['message'] = 'byzentine fault!'
            else:
                if response['status'] == 'success':
                    status = 200
        else:
            # incorrect params in data_json
            response['status'] = 'failure'
            response['message'] = 'invalid data in params'

        response = json.dumps(response)
        return response, status
class MyServer:
    def __init__(self, name, http_host, http_port, grpc_host, grpc_port):
        print("Starting Server..")
        server = MyServerHandler(name, grpc_host, grpc_port)
        print("Server is running on " + http_host + ":" + http_port)
        server.run(http_host, http_port)
