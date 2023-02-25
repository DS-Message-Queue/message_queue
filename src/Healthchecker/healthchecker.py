#program to implement healthchecker which creates 3 tables producer,consumer,broker.
import psycopg2
from datetime import datetime
class HealthChecker():
    def __init__(self):
        self.conn = self.get_connection()
        if self.conn:
            print("Connection to the PostgreSQL established successfully.")
        else:
            print("Connection to the PostgreSQL encountered and error.")

        self.curr = self.conn.cursor()

        self.create_tables()
    #connection establishment for database
    def get_connection(self):
        try:
            return psycopg2.connect(
                database="healthchecker",
                user="postgres",
                password="test123",
                host="localhost",
                port=5432,
            )
        except:
            return False

    def create_tables(self):
        '''This function creates tables for producer, consumer and broker'''
        self.curr = self.conn.cursor()
        
        self.curr.execute("CREATE TABLE IF NOT EXISTS Producer(p_id VARCHAR(255) PRIMARY KEY, time_stamp TIMESTAMP)")
        self.conn.commit()

        self.curr.execute("CREATE TABLE IF NOT EXISTS Consumer(c_id VARCHAR(255) PRIMARY KEY, time_stamp TIMESTAMP)")
        self.conn.commit()

        self.curr.execute("CREATE TABLE IF NOT EXISTS Broker(b_id VARCHAR(255) PRIMARY KEY, time_stamp TIMESTAMP)")
        self.conn.commit()

    def clear_database(self):
        '''This function is used to clear database tables'''
        self.curr = self.conn.cursor()
        self.curr.execute("truncate table Producer, Consumer, Broker;")
        self.conn.commit()




    def insert_into_producer(self, p_id, time_stamp):
        '''This function is used to insert values into  producer database '''
        self.curr = self.conn.cursor()
        
        string = "INSERT INTO Producer(p_id, time_stamp) VALUES(" + str(p_id) + ", '" + time_stamp + "');"
        self.curr.execute(string)
        self.conn.commit()
    
    def insert_into_consumer(self, c_id, time_stamp):
        '''This function is used to insert values into  consumer database '''
        self.curr = self.conn.cursor()

        string = "INSERT INTO Consumer(c_id, time_stamp) VALUES(" + str(c_id) + ", '" + time_stamp + "');"
        self.curr.execute(string)
        self.conn.commit()

    def insert_into_broker(self, b_id, time_stamp):
        '''This function is used to insert values into  broker database '''
        self.curr = self.conn.cursor()

        string = "INSERT INTO Broker(b_id, time_stamp) VALUES(" + str(b_id) + ", '" + time_stamp + "');"
        self.curr.execute(string)
        self.conn.commit()

    def update_into_producer(self, p_id, time_stamp):
        '''This function is used to update timestamp values into  producer database '''
        
        
        self.curr = self.conn.cursor()

        string = "UPDATE Producer SET time_stamp = '" + time_stamp + "' WHERE p_id = '" + str(p_id) + "';"
        self.curr.execute(string)
        self.conn.commit()

    def update_into_consumer(self, c_id, time_stamp):
        '''This function is used to update timestamp values into  consumer database '''
        
        self.curr = self.conn.cursor()

        string = "UPDATE Consumer SET time_stamp = '" + time_stamp + "' WHERE c_id = '" + str(c_id) + "';"
        self.curr.execute(string)
        self.conn.commit()

    def update_into_broker(self, b_id, time_stamp):
        '''This function is used to update timestamp values into  broker database '''
        
        self.curr = self.conn.cursor()
        
        string = "UPDATE Broker SET time_stamp = '" + time_stamp+ "' WHERE b_id = '" + str(b_id) + "';"
        self.curr.execute(string)
        self.conn.commit()

    def __del__(self):
        if self.conn:
            self.conn.close()

