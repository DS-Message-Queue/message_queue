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

        self.token = token
        self.registered = False

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(host, port))

        # bind the client and the server
        self.stub = m_pb2_grpc.ManagerServiceStub(self.channel)
        
        self.__consumer = {}
        self.__topic = {}
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

        self.curr.execute("CREATE TABLE IF NOT EXISTS message(m_id serial, message varchar(255), topic_name VARCHAR(255), subscribers INT, partition_id INT)")
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
        while True:
            try:
                Queries = self.stub.GetUpdates(m_pb2.Request())
                self.curr = self.conn.cursor()
                for q in Queries:
                    query = q.query
                    if 'INSERT INTO message' in query:
                        final_query = 'INSERT INTO message(message, topic_name, partition_id, subscribers) VALUES('
                        length = len(final_query)
                        query = query[length: -1]
                        line = query.split(',')
                        topic_name = line[1].strip()
                        partition_id = line[2].strip()
                        message = line[0].strip()
                        count = 0
                        for consumer in self.__consumer:
                            if topic_name[1:-1] in self.__consumer[consumer]:
                                count += 1

                        final_query += message + ', ' + topic_name + ', ' + partition_id + ', ' + str(count) + ');'
                        query = final_query
                    self.curr.execute(query)
                    self.conn.commit()
                break
            except Exception as e:
                print('exception:', e)
                self.register_replica_if_required()
                continue

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
                self.curr.execute("SELECT message, subscribers, m_id from message where topic_name = '" + str(res[0]) + "' and partition_id = " + str(res[2]) + " order by m_id asc;")
                result_message = self.curr.fetchall()
                message = []
                subscribers = []
                m_id = []
                for mes in result_message:
                    message.append(mes[0])
                    subscribers.append(mes[1])
                    m_id.append(mes[2])

                if str(res[0]) not in self.__consumer[consumer]:
                    self.__consumer[str(consumer)][str(res[0])] = {}
                else:
                    if res[2] not in self.__consumer[consumer][res[0]]:
                        self.__consumer[str(consumer)][str(res[0])][str(res[2])] = {}

                self.__consumer[str(consumer)][str(res[0])][str(res[2])] = {'message' : message, 'm_id': m_id, 'subscribers' : subscribers, 'bias': int(result_for_partition[0][0]), 'position': int(res[1]) }
        

        # print(self.__consumer)

    def insert_for_consumer(self, consumer_id, topic, position, partition):
        self.curr =  self.conn.cursor()
        try:
            for i in range(len(partition)):
                if position == 0:
                    self.curr.execute("INSERT INTO consumer(c_id, topic_name, position, partition_id) VALUES(" + str(consumer_id) + ", '" + topic + "', " + str(0) + ", " + str(partition[i]) + ");")    
                    self.conn.commit()
                else:
                    self.curr.execute("UPDATE message set subscribers = subscribers + 1 WHERE topic_name = '" + topic + "' and partition_id = " + str(partition[i]))            
                    self.conn.commit()
                    self.curr.execute("INSERT INTO consumer(c_id, topic_name, position, partition_id) VALUES(" + str(consumer_id) + ", '" + topic + "', " + str(position) + ", " + str(partition[i]) + ");")

                    self.conn.commit()
                # self.__consumer[consumer_id][topic][partition]['subscribers'][position] += 1

        except:
            print("No results")
            
    def partition_id(self):
        for consumer in self.__consumer:
            self.current_partition[consumer] = 1

    def consumer_register(self, topic):
        
        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        
        self.get_updates()
        topics = []
        self.curr = self.conn.cursor()
        self.curr.execute("SELECT * from topic;")
        result = self.curr.fetchall()

        for res in result:
            res = list(res)
            topics.append(res[0])
        
        if topic not in topics:
            self.__lock.release()
            return raise_error("Topic doesn't exist.")

        self.curr.execute("SELECT partition_id from topic where topic_name = '" + topic + "';")
        result = self.curr.fetchall()
        partition = []
        for res in result:
            res = list(res)
            print(res)
            partition.append(res[0])        

        consumer_id = len(self.__consumer) + 1
        
        position = 0
        if len(self.__consumer) != 0:
            for i in self.__consumer:
                if topic in self.__consumer[i]:
                    for j in self.__consumer[i][topic]:
                        position = len(self.__consumer[i][topic][j]['message'])

        if position == []:
            position = 0

        self.insert_for_consumer(consumer_id, topic, position, partition)
        # self.__consumer = {}

        self.initialize_dict()
        # print(self.__consumer[str(consumer_id)])
        self.partition_id()
        self.__lock.release() 
        # print(self.__consumer)

        return raise_success("Consumer registered successfully.", {"consumer_id": consumer_id})

    def list_topics(self):

        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        self.get_updates()
        topics = []
        self.curr = self.conn.cursor()
        self.curr.execute("SELECT DISTINCT(topic_name) from topic;")
        result = self.curr.fetchall()
        for res in result:
            topics.append(res[0])

        if topics == []:
            self.__lock.release()
            return raise_error("No topics found")

        self.__lock.release()
        return raise_success("Successfully fetched topics.", {"topics": topics})

    def list_partitions(self, topic):

        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        self.get_updates()
        partition = []
        self.curr = self.conn.cursor()
        self.curr.execute("SELECT partition_id from topic where topic_name = '" + topic + "';")
        result = self.curr.fetchall()
        for res in result:
            partition.append(res[0])

        
        if (partition == []):
            self.__lock.release()
            return raise_error("No partitions found")

        self.__lock.release()
        return raise_success("Successfully fetched Partitions for topic - " + topic, {"partitions": partition})

    def consume_message_with_partition(self, topic, consumer_id, partition, lock_acquired = False):
        partition = str(partition)

        if not lock_acquired:
            isLockAvailable = self.__lock.acquire(blocking = False)
            if isLockAvailable is False:
                return raise_error("Lock cannot be acquired.")
        
        if topic not in self.__consumer[consumer_id]:
            if not lock_acquired:
                self.__lock.release()
            return raise_error(consumer_id + " did not subscribe to topic - " + topic)

        # print(self.__consumer[consumer_id])        
        if partition not in self.__consumer[consumer_id][topic]:
            if not lock_acquired:
                self.__lock.release()

            return raise_error("Partition " + partition + " Not Found")
        

        message_position = self.__consumer[consumer_id][topic][partition]['position']
        print(message_position)
        

        if self.__consumer == {}:
            if not lock_acquired:
                self.__lock.release()
            return raise_error("No consumer found")

        # if self.__consumer[consumer_id][topic][partition]['subscribers'][-1] != 0:
        #     message_position += 1
        # print('Mess ', message_position, self.__consumer[consumer_id][topic][partition]['subscribers'][message_position])
        if message_position >= len(self.__consumer[consumer_id][topic][partition]['message']) \
        or self.__consumer[consumer_id][topic][partition]['subscribers'][message_position] == 0:
            self.get_updates()
            if not lock_acquired:
                self.__lock.release()
            return raise_error("No new message is published to " + topic + ", " + partition + ".")
        
        m_id = self.__consumer[consumer_id][topic][partition]['m_id'][message_position]
        message = self.__consumer[consumer_id][topic][partition]['message'][message_position]
        self.__consumer[consumer_id][topic][partition]['position'] = self.__consumer[consumer_id][topic][partition]['position'] + 1
        self.__consumer[consumer_id][topic][partition]['subscribers'][message_position] -= 1

        print('sql call start')
        self.curr = self.conn.cursor()
        self.curr.execute("UPDATE consumer set position = " + str(self.__consumer[consumer_id][topic][partition]['position']) + " WHERE c_id = " + str(consumer_id) + " and partition_id = " + str(partition) + " and topic_name = '" + topic + "';")
        self.conn.commit()
        self.curr.execute("UPDATE message set subscribers = " + str(self.__consumer[consumer_id][topic][partition]['subscribers'][message_position]) + " WHERE topic_name = '" + topic + "' and partition_id = " + str(partition) + " and message = '" + message + "' and m_id = " + str(m_id) + ";")
        self.conn.commit()
        print('sql call end')
        
        # self.__consumer == {}
        # self.initialize_dict()
        if not lock_acquired:
                self.__lock.release()
        
        return raise_success("Message fetched successfully.", {
            "message": message
        })


    def consume_message(self, topic, consumer_id):
        try:
            isLockAvailable = self.__lock.acquire(blocking = False)
            if isLockAvailable is False:
                return raise_error("Lock cannot be acquired.")

            if consumer_id not in self.__consumer:
                self.__lock.release()
                return raise_error("Consumer not found.")
            
            topics = []
            self.curr = self.conn.cursor()
            print('befor cm - 1')
            self.curr.execute("SELECT * from topic;")
            result = self.curr.fetchall()
            print('after cm - 1')
            for res in result:
                res = list(res)
                topics.append(res[0])
            
            if topic not in topics:
                self.get_updates()
                self.__lock.release()
                return raise_error("Topic doesn't exist.")

            print('before cm - 2')
            self.curr.execute("SELECT * from topic WHERE topic_name = '" + topic + "';")
            result = self.curr.fetchall()
            print('after cm - 2 .{}.'.format(topic))
            partition = 1
            
            response = {}
            for res in result:
                partition = res[2]
                response = self.consume_message_with_partition(topic, consumer_id, partition, lock_acquired = True)
                if "No new message is published to" in response["message"]:
                    continue
                else:
                    break
            
            if "No new message is published to" in response["message"]:
                self.get_updates()
                self.__lock.release()
                return raise_error("No new message is published to " + topic + ".")

            print('returning....')
            self.__lock.release()
            return response
        
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
            response = self.manager.consumer_register(transaction['topic'])
            return response

        elif transaction['req'] == 'GetTopics':
            return self.manager.list_topics()

        elif transaction['req'] == 'GetPartition':
            topic = transaction['topic']
            return self.manager.list_partitions(topic)

        elif transaction['req'] == 'DequeueWithPartition':
            topic = transaction['topic']
            consumer_id = transaction['consumer_id']
            partition = transaction['partition']

            response = self.manager.consume_message_with_partition(str(topic), str(consumer_id), str(partition))
            if "No new message is published to" in response["message"] or \
            "Topic doesn't exist." in response["message"]:
                return self.manager.consume_message_with_partition(str(topic), str(consumer_id), str(partition))
            return response

        elif transaction['req'] == 'Dequeue':
            topic = transaction['topic']
            consumer_id = transaction['consumer_id']

            response = self.manager.consume_message(topic, str(consumer_id))
            if "No new message is published to" in response["message"] or \
            "Topic doesn't exist." in response["message"]:
                return self.manager.consume_message(topic, str(consumer_id))
            return response

        elif transaction['req'] == 'ClearDatabase':
            output = {}
            try:
                self.manager.del_database()
                output = {"status": "success",
                          "message": "Database Cleared successfully."}
            except:
                output = {"status": "failure",
                          "message": "Couldn't clear database."}
            return output

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
