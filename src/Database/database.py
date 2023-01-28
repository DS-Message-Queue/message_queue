import psycopg2

class databases:
    
    def __init__(self):
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
                database="d_queue",
                user="postgres",
                password="test123",
                host="localhost",
                port=5432,
            )
        except:
            return False

    def create_tables(self, conn):
        self.curr = self.conn.cursor()

        self.curr.execute("CREATE TABLE IF NOT EXISTS topic(topic_name VARCHAR(255) PRIMARY KEY, bias INT)")
        self.conn.commit()

        self.curr.execute("CREATE TABLE IF NOT EXISTS producer(p_id INT PRIMARY KEY, topic_name VARCHAR(255), FOREIGN KEY(topic_name) REFERENCES topic(topic_name))")
        self.conn.commit()

        self.curr.execute("CREATE TABLE IF NOT EXISTS consumer(c_id INT PRIMARY KEY, topic_name VARCHAR(255),position INT, FOREIGN KEY(topic_name) REFERENCES topic(topic_name))")
        self.conn.commit()

        self.curr.execute("CREATE TABLE IF NOT EXISTS message(message varchar(255), topic_name VARCHAR(255), subscribers INT, FOREIGN KEY(topic_name) REFERENCES topic(topic_name))")
        self.conn.commit()

        #print("Executed successfully")


    def insert_topic(self, topic_name, bias = 0):
        self.curr = self.conn.cursor()

        string = "INSERT INTO topic(topic_name, bias) VALUES('" + topic_name + "', " + str(bias) + ");"
        self.curr.execute(string)
        self.conn.commit()

    def insert_for_producer(self, p_id, topic_name):
        self.curr = self.conn.cursor()

        string = "INSERT INTO PRODUCER(p_id, topic_name) VALUES(" + str(p_id) + ", '" + topic_name + "');"
        self.curr.execute(string)
        self.conn.commit()

    def insert_for_consumer(self, c_id, topic_name, position = 0):
        self.curr = self.conn.cursor()

        string = "INSERT INTO CONSUMER(c_id, topic_name, position) VALUES(" + str(c_id) + ", '" + topic_name + "', " + str(position) + ");"
        self.curr.execute(string)
        self.conn.commit()

    def insert_for_messages(self, topic_name, message, subscribers):
        self.curr = self.conn.cursor()

        string = "INSERT INTO message(message, topic_name, subscribers) VALUES('" + message + "', '" + topic_name + "', " + str(subscribers) + ");"
        self.curr.execute(string)
        self.conn.commit()


    def delete_from_message(self, topic_name):
        
        self.curr = self.conn.cursor()
        string = "DELETE FROM message where topic_name = '" + topic_name + "' ;"
        self.curr.execute(string)
        self.conn.commit()      

    def update_for_consumer(self, cid, position):
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

    def recover_from_crash(self, __topics, __producers, __consumers):
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
                    "messages": [],
                    "bias" : 0
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
            for message in result_message:
                messaging.append({"message" : message[0], "subscribers" : message[2]})

            __topics[topic[0]]["messages"] = messaging
            __topics[topic[0]]["bias"] = topic[1]
            
            

        for producer in result_producer:
            if producer[0] not in __producers:
                __producers[producer[0]] = producer[1]

        
        for consumer in result_consumer:
            topic_dict = {'position' : consumer[2]}
            topics_dict = {'topics' : {consumer[1] : topic_dict}}
            if consumer[0] not in __consumers:
                __consumers[consumer[0]] = topics_dict

        print(__topics, __consumers, __producers)
        return __topics, __producers, __consumers
        


    def __del__(self):
        if self.conn:
            self.conn.close()


# d = databases()



# d.insert_topic("Cricket", 0)
# d.insert_topic("haiii", 0)
# d.insert_for_producer( 123, "Cricket")
# d.insert_for_consumer( 456, "Cricket", 0)
# d.insert_for_messages( "Cricket", "SCORE - 123/10",5)
# d.insert_for_messages( "Cricket", "SCORE - 123/9",5)
# d.insert_for_messages( "haiii", "SCORE - 140/0",3)
# d.update_for_topic("Cricket",1)
# d.update_for_consumer(456, 2)
# d.update_for_message("Cricket", 6)
# print("Insertions done")
# d.delete_from_message("haiii")

# # conn.close()