#program to implement healthchecker which creates 3 tables producer,consumer,broker.
import psycopg2
from datetime import datetime
from src.Database.main_db import databases
class HealthChecker():
    def __init__(self):
        self.conn = databases().get_connection()
        self.create_tables()

    def create_tables(self):
        '''This function creates tables for producer, consumer and broker'''
        self.curr = self.conn.cursor()
        
        self.curr.execute("CREATE TABLE IF NOT EXISTS Producer_t(p_id VARCHAR(255), time_stamp TIMESTAMP)")
        self.conn.commit()

        self.curr.execute("CREATE TABLE IF NOT EXISTS Consumer_t(c_id VARCHAR(255), time_stamp TIMESTAMP)")
        self.conn.commit()

        self.curr.execute("CREATE TABLE IF NOT EXISTS Broker(b_id VARCHAR(255), time_stamp TIMESTAMP)")
        self.conn.commit()

    def clear_database(self):
        '''This function is used to clear database tables'''
        self.curr = self.conn.cursor()
        self.curr.execute("truncate table Producer_t, Consumer_t, Broker;")
        self.conn.commit()




    def insert_into_producer(self, p_id, time_stamp):
        '''This function is used to insert values into  producer database '''
        self.curr = self.conn.cursor()
        
        string = "INSERT INTO Producer_t(p_id, time_stamp) VALUES(" + str(p_id) + ", '" + time_stamp + "');"
        self.curr.execute(string)
        self.conn.commit()
    
    def insert_into_consumer(self, c_id, time_stamp):
        '''This function is used to insert values into  consumer database '''
        self.curr = self.conn.cursor()

        string = "INSERT INTO Consumer_t(c_id, time_stamp) VALUES(" + str(c_id) + ", '" + time_stamp + "');"
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

        string = "UPDATE Producer_t SET time_stamp = '" + time_stamp + "' WHERE p_id = '" + str(p_id) + "';"
        self.curr.execute(string)
        self.conn.commit()

    def update_into_consumer(self, c_id, time_stamp):
        '''This function is used to update timestamp values into  consumer database '''
        
        self.curr = self.conn.cursor()

        string = "UPDATE Consumer_t SET time_stamp = '" + time_stamp + "' WHERE c_id = '" + str(c_id) + "';"
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

