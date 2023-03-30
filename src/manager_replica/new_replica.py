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
        self.__topics = {}
        #Initializing Database Connections

        self.conn = self.get_connection()
        if self.conn:
            print("Connection to the PostgreSQL established successfully.")
        else:
            print("Connection to the PostgreSQL encountered and error.")

        self.curr = self.conn.cursor()
        self.create_tables(self.conn)
        self.__lock = threading.Lock()

        # To make sure get_updates is not queued up 
        # by some other thread while it is already running
        self.__get_updates_lock = threading.Lock()

        self.current_partition = {}
        self.initialize_dict()

        
        
    
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
        self.__topics = {}
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

    def get_updates_thread(self):
        """
            *** Make sure only one thread is running this ***
            *** DO NOT QUEUE UP ANOTHER get_updates if some thread is already running this ***
        """
        
        print('getting updates from manager...')
        while True:
            try:
                Queries = self.stub.GetUpdates(m_pb2.Request())
                topic_name = None
                partition_id = None
                message = None
                m_id = None
                count = 0
                for q in Queries:
                    query = q.query
                    
                    # validate query
                    if "INSERT INTO " not in query or ";" not in query:
                        continue

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

                        final_query += message + ', ' + topic_name + ', ' + partition_id + ', ' + str(count) + ')'
                        query = final_query+" returning m_id;"
                    
                    # acquire lock before updating shared variable
                    self.__lock.acquire()
                    self.curr = self.conn.cursor()
                    self.curr.execute(query)
                    self.conn.commit()
                    if 'INSERT INTO message' in query:
                        m_id = self.curr.fetchone()[0]
                    
                    if 'INSERT INTO topic' in query:
                        words = query[57:-2].split(",")
                        partition_id = words[2].strip()
                        if 'WHERE NOT EXISTS' in query:
                            words = query[56:-2].split(",")
                            partition_id = words[1].strip()[1:-1]
                            
                        topic_name = words[0].strip()[1:-1]
                        if topic_name not in self.__topics:
                            self.__topics[topic_name] = {}
                        if partition_id not in self.__topics[topic_name]:
                            self.__topics[topic_name][partition_id] = {'message':[], 'm_id': [], 'subscribers':[]}

                    elif 'INSERT INTO message' in query:
                        topic_name = topic_name[1:-1]
                        if partition_id not in self.__topics[topic_name]:
                            print('UNACCEPTABLE')
                        self.__topics[topic_name][partition_id]['message'].append(message[1:-1])
                        self.__topics[topic_name][partition_id]['subscribers'].append(count)
                        self.__topics[topic_name][partition_id]['m_id'].append(m_id)

                    else:
                        print('invalid query provided')
                    self.__lock.release()
                break
            except Exception as e:
                print('exception:', e)
                # probably the manager went down, so register again
                self.register_replica_if_required()
                self.__lock.release()
                continue

        print('done.')
        self.__get_updates_lock.release()

    
    def get_updates(self):
        """
            Get updates from the manager
        """
        acquired = self.__get_updates_lock.acquire(blocking=False)
        if not acquired:
            # some thread is already getting updates
            return
        threading.Thread(target=self.get_updates_thread).start()

    
    def initialize_dict(self):
        try:
            self.curr = self.conn.cursor()

            self.curr.execute("SELECT * FROM consumer;")
            result_consumer = self.curr.fetchall()

            for consumer in result_consumer:
                if consumer[0] not in self.__consumer:
                    self.__consumer[str(consumer[0])] = {}

            self.curr.execute("SELECT * FROM topic;")
            result = self.curr.fetchall()      
            for res in result:
                if res[0] not in self.__topics:
                    self.__topics[res[0]] = {}
                    

                self.curr.execute("SELECT * from topic where topic_name = '" + res[0] + "';")
                result_for_topic = self.curr.fetchall()
                message = []
                partition = []
                m_id = []
                for result_topic in result_for_topic:
                    partition.append(result_topic[2])

                for par in partition:
                    if str(par) not in self.__topics[res[0]]:
                        self.__topics[res[0]][str(par)] = {}
                    

                    message = []
                    m_id = []
                    subscribers = []
                    self.curr.execute("SELECT * from message where topic_name = '" + res[0] + "' and partition_id = " + str(par) + " order by m_id asc;")
                    result_for_message = self.curr.fetchall()
                    for result_message in result_for_message:
                        message.append(result_message[1])
                        m_id.append(result_message[0])
                        subscribers.append(result_message[3])

                    self.__topics[res[0]][str(par)] = {'message': message, 'm_id': m_id, 'subscribers': subscribers}

                
            for consumer in self.__consumer:
                self.curr.execute("SELECT topic_name, position, partition_id FROM consumer where c_id = " + str(consumer) + ";")
                resulting = self.curr.fetchall()
                partition = []
                for topic in resulting:
                    partition.append(topic[2])
                    if topic[0] not in self.__consumer[consumer]:
                        self.__consumer[consumer][topic[0]] = {}

                    if str(topic[2]) not in self.__consumer[consumer][topic[0]]:
                        self.__consumer[consumer][topic[0]][str(topic[2])] = {'position': topic[1]}
                
                
                for topic in resulting:
                    self.curr.execute("SELECT topic_name, partition_id FROM topic where topic_name = '" + topic[0] + "';")
                    result_consumer = self.curr.fetchall()
                    if topic[0] not in self.__consumer[consumer]:
                        self.__consumer[consumer][topic[0]] = {}
                    for result_c in result_consumer:
                        if str(result_c[1]) not in self.__consumer[consumer][topic[0]]:
                            if result_c[1] not in partition:
                                # self.curr.execute("INSERT INTO consumer(c_id, topic_name, position, partition_id) \
                                #                 SELECT '" + str(consumer) + "', '" + topic[0] + "', '0', '" + str(result_c[1]) + \
                                #                 "' WHERE NOT EXISTS(SELECT c_id, topic_name, partition_id from consumer WHERE c_id = " + str(consumer) + \
                                #                 " and topic_name = '"+ topic[0] + "' and partition_id = " + str(result_c[1]) + ");")
                                self.curr.execute("INSERT INTO consumer(c_id, topic_name, position, partition_id) VALUES(" + str(consumer) + ", '" + topic[0] + "', " + str(0) + ", " + str(result_c[1]) + ");")    
                                self.conn.commit()
                                self.__consumer[consumer][topic[0]][str(result_c[1])] = {'position': 0}
                            

            
        except Exception as e:
            print('Exception in intialize dict:', e)
                

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
                    self.curr.execute("INSERT INTO consumer(c_id, topic_name, position, partition_id) VALUES(" + str(consumer_id) + ", '" + topic + "', " + str(position[i]) + ", " + str(partition[i]) + ");")

                    self.conn.commit()
                # self.__consumer[consumer_id][topic][partition]['subscribers'][position] += 1

        except:
            print("No results")
            
    def partition_id(self):
        for consumer in self.__consumer:
            self.current_partition[consumer] = 1

    def consumer_register(self, topic):
        self.get_updates()
        # sleep here so that some kind of update is gathered meanwhile
        time.sleep(0.05)

        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")

        self.curr = self.conn.cursor()
        
        if topic not in self.__topics:
            self.__lock.release()
            return raise_error("Topic doesn't exist.")
        
        partition = [i for i in self.__topics[topic]]
        consumer_id = len(self.__consumer) + 1
        
        position = []
        if len(self.__consumer) != 0:
            for topics in self.__topics:
                for j in self.__topics[topic]:
                    position.append(len(self.__topics[topics][j]['message']))

        if position == []:
            position = 0

        self.insert_for_consumer(consumer_id, topic, position, partition)
        if str(consumer_id) not in self.__consumer:
            self.__consumer[str(consumer_id)] = {}
        if topic not in self.__consumer[str(consumer_id)]:
            self.__consumer[str(consumer_id)][topic] = {}
        
        for i in range(len(partition)):
            if str(partition[i]) not in self.__consumer[str(consumer_id)][topic]:
                if position == 0:
                    self.__consumer[str(consumer_id)][topic][partition[i]] = {'position': 0}
                else:
                    self.__consumer[str(consumer_id)][topic][partition[i]] = {'position': position[i]}
        
        self.__lock.release() 
        return raise_success("Consumer registered successfully.", {"consumer_id": consumer_id})


    def list_topics(self):

        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        
        self.get_updates()
        time.sleep(0.05)
        topic = []
        for res in self.__topics:
            topic.append(res)

        if topic == []:
            self.__lock.release()
            return raise_error("No topics found")

        self.__lock.release()
        return raise_success("Successfully fetched topics.", {"topics": topic})

    def list_partitions(self, topic):

        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        self.get_updates()
        time.sleep(0.05)
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
        # partition = str(partition)
        # consumer_id = str(consumer_id)
        if not lock_acquired:
            self.get_updates()

        if not lock_acquired:
            isLockAvailable = self.__lock.acquire(blocking = False)
            if isLockAvailable is False:
                return raise_error("Lock cannot be acquired.")

        if topic not in self.__consumer[consumer_id]:
            if not lock_acquired:
                self.__lock.release()
            return raise_error(consumer_id + " did not subscribe to topic - " + topic)

        if partition not in self.__consumer[consumer_id][topic]:
            if not lock_acquired:
                self.__lock.release()

            # topic exists in self.__consumer[consumer_id] but partition doesn't
            # This is an internal error which should never happen
            return raise_error(consumer_id + " internal partition error - " + topic)
        
        message_position = self.__consumer[consumer_id][topic][partition]['position']

        if message_position >= len(self.__topics[topic][partition]['message']) \
        or self.__topics[topic][partition]['subscribers'][message_position] == 0:
            if not lock_acquired:
                self.__lock.release()
            self.get_updates()
            return raise_error("No new message is published to " + topic + ", " + partition + ".")
        
        m_id = self.__topics[topic][partition]['m_id'][message_position]
        message = self.__topics[topic][partition]['message'][message_position]
        self.__consumer[consumer_id][topic][partition]['position'] = self.__consumer[consumer_id][topic][partition]['position'] + 1
        self.__topics[topic][partition]['subscribers'][message_position] -= 1

        self.curr = self.conn.cursor()
        self.curr.execute("UPDATE consumer set position = " + str(self.__consumer[consumer_id][topic][partition]['position']) + " WHERE c_id = " + str(consumer_id) + " and partition_id = " + str(partition) + " and topic_name = '" + topic + "';")
        self.conn.commit()
        self.curr.execute("UPDATE message set subscribers = " + str(self.__topics[topic][partition]['subscribers'][message_position]) + " WHERE topic_name = '" + topic + "' and partition_id = " + str(partition) + " and message = '" + message + "' and m_id = " + str(m_id) + ";")
        self.conn.commit()

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
            
            if topic not in self.__topics:
                self.__lock.release()
                self.get_updates()
                return raise_error("Topic doesn't exist.")
            
            response = {}
            for partition in self.__topics[topic]:
                response = self.consume_message_with_partition(topic, consumer_id, partition, lock_acquired = True)
                if "No new message is published to" in response["message"]:
                    continue
                else:
                    break
            
            if "No new message is published to" in response["message"]:
                # checked all partitions, still no new message
                self.__lock.release()
                self.get_updates()
                return raise_error("No new message is published to " + topic + ".")

            self.__lock.release()
            return response
        
        except Exception as e:
            print('exception in consume message:', e) 


    def log_size(self, topic_name, consumer_id):
        isLockAvailable = self.__lock.acquire(blocking=False)
        
        if isLockAvailable is False:
            # with open('errorlog.txt', 'a') as f:
            #     f.write("Topic " + topic_name + " doesn't exist. : " + str(self.__topics))
            return raise_error("Lock cannot be acquired.")
        
        self.get_updates()
        if topic_name not in self.__topics:
            self.__lock.release()
            # with open('errorlog.txt', 'a') as f:
            #     f.write("Topic " + topic_name + " doesn't exist. : " + str(self.__topics))
            return raise_error("Topic " + topic_name + " doesn't exist.")
        
        if consumer_id not in self.__consumer:
            self.__lock.release()
            # with open('errorlog.txt', 'a') as f:
            #     f.write(consumer_id + " id Consumer doesn't exist. : " + str(self.__consumer))
            return raise_error("Consumer doesn't exist.")
        
        if topic_name not in self.__consumer[consumer_id]:
            self.__lock.release()
            # with open('errorlog.txt', 'a') as f:
            #     f.write(consumer_id + " id Consumer is not subscribed to " + topic_name + ". : " + str(self.__consumer))
            return raise_error("Consumer is not subscribed to " + topic_name + ".")
        
        size = 0
        for i in self.__topics[topic_name]:
            size += (len(self.__topics[topic_name][i]["message"]) - self.__consumer[consumer_id][topic_name][i]['position'])

        self.__lock.release()
        return raise_success("Successfully fetched size for topic " + topic_name + ".", {"size": size })

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
            # if "No new message is published to" in response["message"] or \
            # "Topic doesn't exist." in response["message"]:
            #     return self.manager.consume_message_with_partition(str(topic), str(consumer_id), str(partition))
            return response

        elif transaction['req'] == 'Dequeue':
            topic = transaction['topic']
            consumer_id = transaction['consumer_id']

            response = self.manager.consume_message(topic, str(consumer_id))
            # if "No new message is published to" in response["message"] or \
            # "Topic doesn't exist." in response["message"]:
            #     return self.manager.consume_message(topic, str(consumer_id))
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
    
        elif transaction['req'] == 'Size':
            topic = transaction['topic']
            consumer_id = transaction['consumer_id']

            return self.manager.log_size(topic, str(consumer_id))

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
