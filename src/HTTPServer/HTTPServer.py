from http.server import HTTPServer, BaseHTTPRequestHandler
from src.controller.main import Message_Queue
import json
import re

message_queue = Message_Queue()

# returns None in case of error
def extract_url_params(data):
    i = 0
    params = {}
    key = ""
    value = ""
    extracting_key = True
    while i < len(data):
        if data[i] == '&':
            if len(value) == 0:
                return None
            else:
                params[key] = value
                key = ""
                value = ""
                extracting_key = True
        elif data[i] == '=':
            if len(key) == 0:
                return None
            else:
                extracting_key = False
        else:
            if extracting_key:
                key += data[i]
            else:
                value += data[i]
        i += 1
    
    # should be extracting value at this point
    if extracting_key or len(value) == 0:
        return None
    
    params[key] = value
    return params

class MyServerHandler(BaseHTTPRequestHandler):
    def __init__(self, *args):
        # Connect to db server here and store necessary variables in self
        # Be aware that this gets called everytime an HTTP request is made
        # So maybe there is a better place for it

        BaseHTTPRequestHandler.__init__(self, *args)

    # def __del__(self):
    #    # close connection to db here

    def do_GET(self):
        # ListTopics
        if self.path == "/topics":
            print('topics requested')
            # build response
            response = {}

            # retrieve topics
            ret = message_queue.list_topics()
            # topics = ['post_created', 'user_signup', 'user_login']
            
            # if True: # Status
            #     response['status'] = 'success'
            #     response['topics'] = topics
            #     self.send_response(200)

                # Make sure to remove above 4 lines because get_topics() should
                # return the required response, so do this here instead:
            response.update(ret)
            if response['status'] == 'success':
                self.send_response(200)
            else:
                self.send_response(400)

            # else:
            #     response['status'] = 'failure'
            #     response['message'] = 'failure messasge'
            #     self.send_response(400)

            response = json.dumps(response)
            self.send_header("Content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(bytes(response, "utf-8"))

        # dequeue
        elif re.match("^(/consumer/consume)", self.path) != None:
            print("dequeue requested")
            json_is_valid = True
            data_json = extract_url_params(self.path[18:])
            response = {}
            if data_json is None:
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
                # message = "test message"
                
                # if True: # Status
                #     response['status'] = 'success'
                #     response['message'] = message
                #     self.send_response(200)

                # Make sure to remove above 4 lines because consumer_dequeue() should
                # return the required response, so do this here instead:
                response.update(ret)
                if response['status'] == 'success':
                    self.send_response(200)
                else:
                    self.send_response(400)
                
            else:
                # incorrect params in data_json
                response['status'] = 'failure'
                response['message'] = 'invalid data in params'
                self.send_response(400)
                
            response = json.dumps(response)
            self.send_header("Content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(bytes(response, "utf-8"))

        # size
        elif re.match("^(/size)", self.path) != None:
            print("size requested")
            json_is_valid = True
            data_json = extract_url_params(self.path[6:])
            response = {}
            if data_json is None:
                print('JSON decode failed')
                json_is_valid = False

            if json_is_valid and len(data_json) == 2 and 'topic' in data_json \
                                                     and 'consumer_id' in data_json:
                topic        =  data_json['topic']
                consumer_id  =  int(data_json['consumer_id'])

                # size
                ret = message_queue.log_size(topic,consumer_id)
                # size = 12
                
                # if True: # Status
                #     response['status'] = 'success'
                #     response['size'] = size
                #     self.send_response(200)

                # Make sure to remove above 4 lines because consumer_queue_size() should
                # return the required response, so do this here instead:
                response.update(ret)
                if response['status'] == 'success':
                    self.send_response(200)
                else:
                    self.send_response(400)
                
            else:
                # incorrect params in data_json
                response['status'] = 'failure'
                response['message'] = 'invalid data in params'
                self.send_response(400)
                
            response = json.dumps(response)
            self.send_header("Content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(bytes(response, "utf-8"))

        elif re.match('^/cleardb', self.path) != None:
            print('clear requested')
            params = extract_url_params(self.path[9:])
            print('params =', params)
            response = {}
            if 'code' in params and params['code'] == 'xBjfq12nh':
                message_queue.clear_database()
                response['status'] = 'success'
                self.send_response(200)
            else:
                response['status'] = 'failure'
                self.send_response(400)
            
            response = json.dumps(response)
            self.send_header("Content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(bytes(response, "utf-8"))

        else:
            print("invalid path request")

    
    def do_POST(self):
        # CreateTopic
        if self.path == "/topics":
            print('create topic requested')
            length = int(self.headers['content-length'])
            data = self.rfile.read(length)
            response = {}
            json_is_valid = True
            data_json = None
            try:
                data_json = json.loads(data)
            except json.decoder.JSONDecodeError:
                print('JSON decode failed')
                json_is_valid = False

            if json_is_valid and len(data_json) == 1 and 'topic_name' in data_json:
                topic_name = data_json['topic_name']

                # create and store topic
                # ret = create_topic(topic_name)
                ret = message_queue.add_topic(topic_name)

                # Commented as mentioned
                # if True: # Status
                #     response['status'] = 'success'
                #     response['message'] = 'Topic ' + topic_name + ' successfully created'
                #     self.send_response(200)

                    # Make sure to remove above 4 lines because create_topic() should
                    # return the required response, so do this here instead:
                response.update(ret)
                if response['status'] == 'success':
                    self.send_response(200)
                else:
                    self.send_response(400)
                
            else:
                # only accept topic_name in data_json
                response['status'] = 'failure'
                response['message'] = 'invalid data in params'
                self.send_response(400)
                
            response = json.dumps(response)
            self.send_header("Content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(bytes(response, "utf-8"))

        # consumer register
        elif self.path == "/consumer/register":
            print("consumer register requested")
            length = int(self.headers['content-length'])
            data = self.rfile.read(length)
            response = {}
            json_is_valid = True
            data_json = None
            try:
                data_json = json.loads(data)
            except json.decoder.JSONDecodeError:
                print('JSON decode failed')
                json_is_valid = False

            if json_is_valid and len(data_json) == 1 and 'topic' in data_json:
                topic = data_json['topic']

                # generate consumer_id
                ret = message_queue.register_consumer(topic)
                
                # consumer_id = 14328048
                # if True: # Status
                #     response['status'] = 'success'
                #     response['consumer_id'] = consumer_id
                #     self.send_response(200)

                    # Make sure to remove above 4 lines because consumer_register() should
                    # return the required response, so do this here instead:
                response.update(ret)
                if response['status'] == 'success':
                    self.send_response(200)
                else:
                    self.send_response(400)
                    
            else:
                # only accept one topic in data_json
                response['status'] = 'failure'
                response['message'] = 'invalid data in params'
                self.send_response(400)
                
            response = json.dumps(response)
            self.send_header("Content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(bytes(response, "utf-8"))

        # producer register
        elif self.path == "/producer/register":
            print("producer register requested")
            length = int(self.headers['content-length'])
            data = self.rfile.read(length)
            response = {}
            json_is_valid = True
            data_json = None
            try:
                data_json = json.loads(data)
            except json.decoder.JSONDecodeError:
                print('JSON decode failed')
                json_is_valid = False

            if json_is_valid and len(data_json) == 1 and 'topic' in data_json:
                topic = data_json['topic']

                # generate producer_id
                ret = message_queue.register_producer(topic)
                
                # producer_id = 14328040
                # if True: # Status
                #     response['status'] = 'success'
                #     response['producer_id'] = producer_id
                #     self.send_response(200)

                    # Make sure to remove above 4 lines because producer_register() should
                    # return the required response, so do this here instead:
                response.update(ret)
                if response['status'] == 'success':
                    self.send_response(200)
                else:
                    self.send_response(400)
                
            else:
                # only accept one topic in data_json
                response['status'] = 'failure'
                response['message'] = 'invalid data in params'
                self.send_response(400)
                
            response = json.dumps(response)
            self.send_header("Content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(bytes(response, "utf-8"))

        # enqueue
        elif self.path == "/producer/produce":
            print("produce requested")
            length = int(self.headers['content-length'])
            data = self.rfile.read(length)
            response = {}
            json_is_valid = True
            data_json = None
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
                
                # if True: # Status
                #     response['status'] = 'success'
                #     self.send_response(200)

                    # Make sure to remove above 3 lines because producer_enqueue() should
                    # return the required response, so do this here instead:
                response.update(ret)
                if response['status'] == 'success':
                    self.send_response(200)
                else:
                    self.send_response(400)
                
            else:
                # incorrect params in data_json
                response['status'] = 'failure'
                response['message'] = 'invalid data in params'
                self.send_response(400)
                    
            response = json.dumps(response)
            self.send_header("Content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(bytes(response, "utf-8"))

        else:
            print("invalid path request")

class MyServer:
    def __init__(self):
        print("Starting Server..")
        self.server = HTTPServer(("127.0.0.1", 8002), MyServerHandler)
        print("Server is running on 127.0.0.1:8002")
        self.server.serve_forever()

    def terminate_server(self):
        self.server.shutdown()
