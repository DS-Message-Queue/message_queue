import logging
import datetime
import threading

class WriteAheadLog():

    def __init__(self):
        self.__lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        self.counter = 0
        self.logger.setLevel(logging.INFO)
        self.formatter = logging.Formatter('%(asctime)s - %(message)s')               
        self.file_handler = logging.FileHandler('WAL.log')
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)

    def logEvent(self, request_type, *args) -> str:
        '''Takes broker id, request type from producer or consumer and logs the corresponding event with sutable args in the log file
            Returns the transaction id for the log'''
        self.__lock.acquire()
        dt = datetime.datetime.now()
        unix = int(dt.timestamp())
        txn_id = str(hex(unix)) + str(self.counter)
        txn_id = txn_id[2:]
        self.counter += 1
        log = "{} - {} - ".format(str(txn_id), str(request_type))+ " - ".join(map(str,args))
        self.logger.info(log)
        self.__lock.release()
        return txn_id
        
    def logSuccess(self, txn_id, request_type):
        '''Takes broker id, request type from producer or consumer and logs the corresponding event with sutable args in the log file with a Success message'''
        self.__lock.acquire()
        log = "{} - {} - ".format(str(txn_id), str(request_type) + " Success")
        self.logger.info(log)
        self.__lock.release()
        
    def clearlogfile(self):
        '''clears the log file. To be called after a checkpoint'''
        with open('WAL.log', 'w') as f:
            pass
