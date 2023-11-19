import psycopg2


class databases:
    
    def __init__(self):
        #Initializing Database Connections

        self.conn = self.get_connection()
        if self.conn:
            print("Connection to the PostgreSQL established successfully.")
        else:
            print("Connection to the PostgreSQL encountered and error.")

        self.curr = self.conn.cursor()

        self.create_tables(self.conn)
    
    def get_connection(self):
        try:
            return psycopg2.connect(
                database="main_db",
                user="postgres",
                password="avCurry@30",
                host="localhost",
                port=5432,
            )
        except Exception as e:
            print('db connection error:', e)
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

    # def insert_for_broker(self, b_id, ip):
    #     self.curr = self.conn.cursor()

    #     string = "INSERT INTO broker(b_id, ip_address) VALUES('" + str(b_id) + "', '" + ip + "');"
    #     self.curr.execute(string)
    #     self.conn.commit()

    # def insert_for_broker_topic(self, b_id, topic_name, partition_id):
    #     self.curr = self.conn.cursor()

    #     string = "INSERT INTO broker_topic(b_id, topic_name, partition_id) VALUES('" + str(b_id) + "', '" + topic_name + "', " + str(partition_id) + ");"
    #     self.curr.execute(string)
    #     self.conn.commit()

    def insert_topic(self, topic_name, partition_id, bias = 0):
        #Inserting into topic
        self.curr = self.conn.cursor()

        string = "INSERT INTO topic(topic_name, bias, partition_id) VALUES('" + topic_name + "', " + str(bias) + ", " + str(partition_id) + ");"
        self.curr.execute(string)
        self.conn.commit()
        return string

    def insert_for_producer(self, p_id, topic_name, partition_id):
        #Inserting into producer
        self.curr = self.conn.cursor()

        string = "INSERT INTO PRODUCER(p_id, topic_name, partition_id) VALUES(" + str(p_id) + ", '" + topic_name + "', " + str(partition_id) + ");"
        self.curr.execute(string)
        self.conn.commit()
        return string

    def insert_for_consumer(self, c_id, topic_name, partition_id, position = 0):
        #Inserting into consumer
        self.curr = self.conn.cursor()

        string = "INSERT INTO CONSUMER(c_id, topic_name, position, partition_id) VALUES(" + str(c_id) + ", '" + topic_name + "', " + str(position) + "," + str(partition_id) + ");"
        self.curr.execute(string)
        self.conn.commit()
        return string

    def insert_for_messages(self,topic_name, message, subscribers, partition_id):
        #Inserting for messages
        self.curr = self.conn.cursor()

        string = "INSERT INTO message(message, topic_name, subscribers, partition_id) VALUES('" + message + "', '" + topic_name + "', " + str(subscribers) + ", " + str(partition_id) + ");"
        self.curr.execute(string)
        self.conn.commit()
        return string

    def run_query(self,query):
        # Running a pre made query, the query can be anything.
        self.curr = self.conn.cursor()
        self.curr.execute(query)
        self.conn.commit()

    def delete_from_message(self, message):
        #Deletion of Messages when there are no subscribers        
        self.curr = self.conn.cursor()
        string = "DELETE FROM message where subscribers = 0;"
        self.curr.execute(string)
        self.conn.commit()      

    def update_for_consumer(self, cid, position):
        #Updating consumer message position in Queue 
        self.curr = self.conn.cursor()

        string = "UPDATE consumer SET position = " + str(position) + " WHERE c_id = " + str(cid) + ";"
        self.curr.execute(string)
        self.conn.commit()

    def update_for_topic(self, topic_name, bias):
        self.curr = self.conn.cursor()

        string = "UPDATE topic SET bias = " + str(bias) + " WHERE topic_name = '" + topic_name + "';"
        self.curr.execute(string)
        self.conn.commit()    

    def update_for_message(self, message, subscribers):
        self.curr = self.conn.cursor()

        string = "UPDATE message SET subscribers = " + str(subscribers) + " WHERE message = '" + message + "';"
        self.curr.execute(string)
        self.conn.commit() 

    def clear_database(self):
        self.curr = self.conn.cursor()
        self.curr.execute("truncate table topic, consumer, producer, message;")
        self.conn.commit()
        
    def recover_from_crash(self, __topics, __producers, __consumers):
        #Database recovery from crash
        self.curr = self.conn.cursor()

        self.curr.execute("SELECT * from topic;")
        result_topic = self.curr.fetchall()

        self.curr.execute("SELECT * from producer;")
        result_producer = self.curr.fetchall()

        self.curr.execute("SELECT * from consumer;")
        result_consumer = self.curr.fetchall()

        self.curr.execute("SELECT * from message;")
        result_message = self.curr.fetchall()

        for topic in result_topic:
            if topic[0] not in __topics:
                __topics[topic[0]] = {
                    "producers": [],
                    "consumers": [],
                    topic[2] : {
                        "messages": [],
                        "bias" : 0
                    }
                }
            
            self.curr.execute("SELECT * from producer WHERE topic_name = '" + topic[0] + "'; ")
            result_producing = self.curr.fetchall()
            produce = []
            for producer in result_producing:
                produce.append(producer[0])
            
            __topics[topic[0]]["producers"] = produce

            self.curr.execute("SELECT * from consumer WHERE topic_name = '" + topic[0] + "'; ")
            result_consuming = self.curr.fetchall()
            consume = []            
            for consumer in result_consuming:
                consume.append(consumer[0])

            __topics[topic[0]]["consumers"] = consume
            
            self.curr.execute("SELECT * from message WHERE topic_name = '" + topic[0] + "'; ")
            result_messaging = self.curr.fetchall()
            messaging = []
            for message in result_messaging:
                messaging.append({"message" : message[0], "subscribers" : message[2]})

            __topics[topic[0]][topic[2]] = {
                "messages" : messaging,
                "bias" : topic[1]
            }
            

        for producer in result_producer:
            if producer[0] not in __producers:
                __producers[producer[0]] = {"topic" : producer[1]}

        
        for consumer in result_consumer:
            topic_dict = {'position' : consumer[2]}
            topics_dict = {'topics' : {consumer[1] : topic_dict}}
            if consumer[0] not in __consumers:
                __consumers[consumer[0]] = topics_dict
        
        dictionary = {}
        for i in reversed(__topics.keys()):
            dictionary[i] = __topics[i]

        
        __topics = dictionary
        
        return __topics, __producers, __consumers
        


    def __del__(self):
        if self.conn:
            self.conn.close()



# __db = databases()
# print("Create tables success")
# cnt = 1
# i = 1
# for t in ["T-1", "T-2", "T-3"]:
#         for j in [1,2,3]:
#             __db.insert_topic(t, j)
#             __db.insert_for_producer(i, t, j)
#             __db.insert_for_consumer(i, t, j)
#             __db.insert_for_messages(t, "Meesagex - " + str(cnt), 5, j)
#             cnt += 1
#         i += 1