from flask import Flask, request
from src.controller.main import Message_Queue
import src.protos.managerservice_pb2_grpc as pb2_grpc
import src.protos.managerservice_pb2 as pb2
import grpc
import json
import re


message_queue = Message_Queue()

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
            data = bytes(json.dumps(transaction).encode('utf-8'))
        ))
        response = json.loads(Response.data)
        print(response)
        #return response

class MyServerHandler:
    def __init__(self, name):
        # Connect to db server here and store necessary variables in self
        # Be aware that this gets called everytime an HTTP request is made
        # So maybe there is a better place for it

        self.manager_rpc = ManagerConnection('localhost', '50051')

        self.app = Flask(name)
        
        @self.app.route('/topics')
        def __get_topics():
            return self.get_topics()
        
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
        # build response
        response = {}

        # retrieve topics
        ret = message_queue.list_topics()
        
        response.update(ret)

        #------------------------------------------------------------------
        # Example usage

        # transaction = {'req': 'Enqueue', 'pid': 12, 'topic': 'foo', 'partition': 'B', 'message': 'test'}
        # self.manager_rpc.send_transaction(transaction)

        #------------------------------------------------------------------

        status = 400
        if response['status'] == 'success':
            status = 200

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

        if json_is_valid and len(data_json) == 2 and 'topic' in data_json \
                                                    and 'consumer_id' in data_json:
            topic        =  data_json['topic']
            consumer_id  =  int(data_json['consumer_id'])

            # dequeue
            ret = message_queue.consume_message(topic, consumer_id)
            
            response.update(ret)
            
            if response['status'] == 'success':
                status = 200
            
        else:
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
            ret = message_queue.log_size(topic,consumer_id)
            
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
            message_queue.clear_database()
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
            topic_name = data_json['topic_name']

            # create and store topic
            ret = message_queue.add_topic(topic_name)

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
            ret = message_queue.register_consumer(topic)
            
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
            ret = message_queue.register_producer(topic)
            
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
            topic        =  data_json['topic']
            producer_id  =  data_json['producer_id']
            message      =  data_json['message']

            # enqueue
            ret = message_queue.publish_message(producer_id, topic, message)
            
            response.update(ret)
            if response['status'] == 'success':
                status = 200
            
        else:
            # incorrect params in data_json
            response['status'] = 'failure'
            response['message'] = 'invalid data in params'
                
        response = json.dumps(response)
        return response, status
    

class MyServer:
    def __init__(self, name):
        print("Starting Server..")
        server = MyServerHandler(name)
        print("Server is running on 127.0.0.1:8002")
        server.run('127.0.0.1', '8002')
