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
import psycopg2
import threading
from src.controller.utils import raise_error, raise_success


partition_id = 1

class ManagerConnection:
    """
    Client for gRPC functionality
    """

    def __init__(self, host, port, token):

        print("Constructor called")
        self.token = token
        self.registered = False

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(host, port))

        # bind the client and the server
        self.stub = m_pb2_grpc.ManagerServiceStub(self.channel)
        
        self.__consumer = {}
        #Initializing Database Connections

        self.conn = self.get_connection()
        if self.conn:
            print("Connection to the PostgreSQL established successfully.")
        else:
            print("Connection to the PostgreSQL encountered and error.")

        self.curr = self.conn.cursor()
        self.create_tables(self.conn)
        self.__lock = threading.Lock()

        self.current_partition = {}

        
    
    def get_connection(self):
        try:
            return psycopg2.connect(
                database="read_replica",
                user="postgres",
                password="test123",
                host="localhost",
                port=5432,
            )
        except:
            return False

    def create_tables(self, conn):
        # Creating Tables topic, consumer, producer, message
        self.curr = self.conn.cursor()

        self.curr.execute("CREATE TABLE IF NOT EXISTS topic(topic_name VARCHAR(255), bias INT, partition_id INT)")
        self.conn.commit()

        self.curr.execute("CREATE TABLE IF NOT EXISTS producer(p_id INT, topic_name VARCHAR(255), partition_id INT)")
        self.conn.commit()

        self.curr.execute("CREATE TABLE IF NOT EXISTS consumer(c_id INT, topic_name VARCHAR(255),position INT, partition_id INT)")
        self.conn.commit()

        self.curr.execute("CREATE TABLE IF NOT EXISTS message(message varchar(255), topic_name VARCHAR(255), subscribers INT, partition_id INT)")
        self.conn.commit()

    def del_database(self):
        self.curr = self.conn.cursor()
        self.curr.execute("truncate table topic, consumer, producer, message;")
        self.conn.commit()
        self.__consumer = {}
        self.current_partition = {}

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
        print('getting updates from manager...')
        while True:
            try:
                Queries = self.stub.GetUpdates(m_pb2.Request())
                self.curr = self.conn.cursor()
                for q in Queries:
                    self.curr.execute(q.query)
                    self.conn.commit()
                break
            except:
                self.register_replica_if_required()
                continue
        print('done.')

        self.initialize_dict()

    
    def initialize_dict(self):
        self.curr = self.conn.cursor()

        self.curr.execute("SELECT * FROM consumer;")
        result_consumer = self.curr.fetchall()

        for consumer in result_consumer:
            if consumer[0] not in self.__consumer:
                self.__consumer[str(consumer[0])] = {}

        for consumer in self.__consumer:
            self.curr.execute("SELECT topic_name, position, partition_id FROM consumer where c_id = " + str(consumer) + ";")
            result = self.curr.fetchall()
            for res in result:
                self.curr.execute("SELECT bias FROM topic where topic_name = '" + str(res[0]) + "' and partition_id = " + str(res[2]))
                result_for_partition = self.curr.fetchall()
                self.curr.execute("SELECT message, subscribers from message where topic_name = '" + str(res[0]) + "' and partition_id = " + str(res[2]))
                result_message = self.curr.fetchall()
                message = []
                subscribers = []
                for mes in result_message:
                    message.append(mes[0])
                    subscribers.append(mes[1])

                if str(res[0]) not in self.__consumer[consumer[0]]:
                    self.__consumer[str(consumer[0])][str(res[0])] = {}
                else:
                    if res[2] not in self.__consumer[consumer[0]][res[0]]:
                        self.__consumer[str(consumer[0])][str(res[0])][str(res[2])] = {}

                self.__consumer[str(consumer[0])][str(res[0])][str(res[2])] = {'message' : message, 'subscribers' : subscribers, 'bias': int(result_for_partition[0][0]), 'position': int(res[1]) }

        # print(self.__consumer)

    def insert_for_consumer(self, consumer_id, topic, position, partition):
        self.curr =  self.conn.cursor()

        for i in range(len(partition)):
            print(position)
            if position == 0:
                self.curr.execute("INSERT INTO consumer(c_id, topic_name, position, partition_id) VALUES(" + str(consumer_id) + ", '" + topic + "', " + str(0) + ", " + str(partition[i]) + ");")    
            else:
                self.curr.execute("UPDATE message set subscribers = subscribers + 1 WHERE topic_name = '" + topic + "' and partition_id = " + str(partition[i]) + ";")            
                self.conn.commit()
                self.curr.execute("INSERT INTO consumer(c_id, topic_name, position, partition_id) VALUES(" + str(consumer_id) + ", '" + topic + "', " + str(position[i]) + ", " + str(partition[i]) + ");")

            self.conn.commit()
            # self.__consumer[consumer_id][topic][partition]['subscribers'][position] += 1
            
    def partition_id(self):
        for consumer in self.__consumer:
            self.current_partition[consumer] = 1

    def consumer_register(self, topic):
        topics = []
        self.curr = self.conn.cursor()
        self.curr.execute("SELECT * from topic;")
        result = self.curr.fetchall()

        for res in result:
            res = list(res)
            topics.append(res[0])

        self.curr.execute("SELECT partition_id from topic where topic_name = '" + topic + "';")
        result = self.curr.fetchall()
        partition = []
        for res in result:
            res = list(res)
            print(res)
            partition.append(res[0])

        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        
        if topic not in topics:
            self.__lock.release()
            return raise_error("Topic doesn't exist.")

        consumer_id = len(self.__consumer) + 1
        

        position = []
        if len(self.__consumer) != 0:
            partition = []
            for i in self.__consumer:
                if topic in self.__consumer[i]:
                    for j in self.__consumer[i][topic]:
                        partition.append(j)
                        position.append(len(self.__consumer[i][topic][j]['message']))

        if position == []:
            position = 0

        self.insert_for_consumer(consumer_id, topic, position, partition)
        # self.__consumer == {}
        self.initialize_dict()
        self.partition_id()
        self.__lock.release() 
        print(self.__consumer)

        return raise_success("Consumer registered successfully.", {"consumer_id": consumer_id})

    def list_topics(self):
        self.get_updates()
        topics = []
        self.curr = self.conn.cursor()
        self.curr.execute("SELECT DISTINCT(topic_name) from topic;")
        result = self.curr.fetchall()
        for res in result:
            topics.append(res[0])

        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")

        self.__lock.release()
        if topics == []:
            return raise_error("No topics found")
        return raise_success("Successfully fetched topics.", {"topics": topics})

    def list_partitions(self, topic):
        self.get_updates()
        partition = []
        self.curr = self.conn.cursor()
        self.curr.execute("SELECT partition_id from topic where topic_name = '" + topic + "';")
        result = self.curr.fetchall()
        for res in result:
            partition.append(res[0])

        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")

        self.__lock.release()
        if (partition == []):
            return raise_error("No partitions found")
        return raise_success("Successfully fetched Partitions for topic - " + topic, {"partitions": partition})

    def consume_message_with_partition(self, topic, consumer_id, partition, depth = 0):


        isLockAvailable = self.__lock.acquire(blocking = False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")

        if partition not in self.__consumer[consumer_id][topic]:
            self.__lock.release()
            return raise_error("Partition Not Found")

        message_position = self.__consumer[consumer_id][topic][partition]['position']
        print(message_position)

        if self.__consumer == {}:
            self.__lock.release()
            # self.get_updates()
            return raise_error("No consumer found")

        if message_position >= len(self.__consumer[consumer_id][topic][partition]['message']):
            self.__lock.release()
            self.get_updates()

            if depth == 2:
                return raise_error("No new message is published to " + topic_name + ", " + partition + ".")
            return self.consume_message_with_partition(topic, consumer_id, partition, depth + 1)
            

        if self.__consumer[consumer_id][topic][partition]['subscribers'][message_position] == 0:
            self.__lock.release()

            self.get_updates()
            if depth == 2:
                return raise_error("No subscribers found")
            
            return self.consume_message_with_partition(topic, consumer_id, partition, depth + 1)
        

        message = self.__consumer[consumer_id][topic][partition]['message'][message_position]
        self.__consumer[consumer_id][topic][partition]['position'] = self.__consumer[consumer_id][topic][partition]['position'] + 1
        self.__consumer[consumer_id][topic][partition]['subscribers'][message_position] -= 1

        self.curr = self.conn.cursor()
        self.curr.execute("UPDATE consumer set position = " + str(self.__consumer[consumer_id][topic][partition]['position']) + " WHERE c_id = " + str(consumer_id) + " and partition_id = " + str(partition) + " and topic_name = '" + topic + "';")
        self.conn.commit()
        self.curr.execute("Update message set subscribers = " + str(self.__consumer[consumer_id][topic][partition]['subscribers'][message_position]) + " WHERE topic_name = '" + topic + "' and partition_id = " + str(partition) + ";")
        self.conn.commit()
        
        # self.__consumer == {}
        self.initialize_dict()
        print(self.__consumer)
        self.__lock.release()
        return raise_success("Message fetched successfully.", {
            "message": message
        })


    def consume_message(self, topic, consumer_id, depth = 0):
        try:
            if consumer_id not in self.__consumer:
                return raise_error("Consumer not found.")

            partition = str(self.current_partition[consumer_id])
            self.current_partition[consumer_id] += 1
            #print(type(self.__consumer[consumer_id]['current_partition']))
            while self.consume_message_with_partition(topic, consumer_id, partition) == {"status": "failure", "message": "Messages exhausted"}:
                partition = str(self.current_partition[consumer_id])
                self.current_partition[consumer_id] += 1

            if self.consume_message_with_partition(topic, consumer_id, partition) == {'status': 'failure', 'message': 'Partition Not Found'}:
                if depth == 2:
                    return raise_error("Messages in all partitions exhausted")

                self.get_updates()
                return self.consume_message(topic, consumer_id, depth + 1)
            
            return self.consume_message_with_partition(topic, consumer_id, partition)
        except Exception as e:
            print('exception:', e) 


    def __del__(self):
        if self.conn:
            print("Closing db")
            self.conn.close()


class ManagerReplicaService(m_pb2_grpc.ManagerServiceServicer):

    def __init__(self):
        with open('./src/manager_replica/replica.json', 'r') as config_file:
            self.config = json.load(config_file)

        self.manager = ManagerConnection(
            self.config['server_host'], 
            self.config['server_port'], 
            self.config['token']
        )

        
        # register replica at the manager
        self.manager.register_replica_if_required()

        # get updates from the manager
        self.manager.get_updates()

    def SendTransaction(self, transaction_req, context):
        print("SendTransaction")
        transaction = json.loads(transaction_req.data)
        response = self.process_transaction(transaction)
        return m_pb2.TransactionResponse(data=json.dumps(response).encode('utf-8'))
    
    def process_transaction(self, transaction):
        print(transaction)
        
        if transaction['req'] == "ConsumerRegister":
            # self.manager.get_updates()
            topic = transaction['topic']
                    
            # Generating unique Id for a consumer
            
            response = self.manager.consumer_register(topic)
            if response['message'] == "Topic doesn't exist.":
                self.manager.get_updates()
                return self.manager.consumer_register(topic)
            return response

        elif transaction['req'] == 'GetTopics':
            # self.manager.get_updates()

            return self.manager.list_topics()

        elif transaction['req'] == 'GetPartition':
            # self.manager.get_updates()

            topic = transaction['topic']
            return self.manager.list_partitions(topic)

        elif transaction['req'] == 'DequeueWithPartition':
            # self.manager.get_updates()
            topic = transaction['topic']
            consumer_id = transaction['consumer_id']
            partition = transaction['partition']

            return self.manager.consume_message_with_partition(str(topic), str(consumer_id), str(partition))

        elif transaction['req'] == 'Dequeue':
            topic = transaction['topic']
            consumer_id = transaction['consumer_id']

            return self.manager.consume_message(topic, str(consumer_id))

        elif transaction['req'] == 'ClearDatabase':
            try:
                self.manager.del_database()
                output = {"status": "success",
                          "message": "Database Cleared successfully."}
            except:
                output = {"status": "failure",
                          "message": "Couldn't clear database."}
            self.__lock.release()
            return m_pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

class ManagerReplica:
    def __init__(self, name, http_host, http_port, grpc_host, grpc_port):

        # HTTP Endpoint
        
        t = multiprocessing.Process(target=self.serve_endpoint, args=(
            name, http_host, http_port, grpc_host, grpc_port
        ))
        t.start()
        # start grpc server for replica
        server_thread = multiprocessing.Process(target=self.serve_grpc)
        server_thread.start()
        
        # self.serve_endpoint(name, http_host, http_port, grpc_host, grpc_port)
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
