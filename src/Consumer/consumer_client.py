import requests

HOST = "http://127.0.0.1:"
PORT = "8001"

class MyConsumerError(Exception):
    '''Error in consumer'''

class MyConsumer():

    def __init__(self, consumerId = None):
        #if the consumerId is already known, the consumer can directly access the topics and logs
        self.__topics = {}
        
    def RegisterConsumer(self, topic):
        '''Registers the consumer to a topic and assign the topic specific consuerId
            Inorder to subscribe to multiple topics call this function multiple times'''
        if topic in self.__topics:
            raise MyConsumerError('Already registered for this topic.')
        
        API_ENDPOINT = "/consumer/register"
        url = HOST+PORT+API_ENDPOINT
        payload = {"topic" : topic}
        
        r = requests.post(url, json = payload)
        data = r.json()
        #try untill the lock is aquired (maximum 30 times then raise error)
        tries = 30
        while tries:
            if r.status_code == 400 and "Lock cannot be acquired." in r.text:
                r = requests.post(url, json = payload)
                data = r.json()
            else:
                break
            tries -= 1
        else:
            raise MyConsumerError('Server is busy, please try again after a while.')
        if r.status_code == 200:
            # store the consumerId returned by the server
            self.__topics[topic] = data['consumer_id']  
        elif r.status_code == 400:
            raise MyConsumerError(data['message'])
        
    def ListTopics(self) -> list:
        '''Returns the list of all the topics available that the producers have created'''
        API_ENDPOINT = "/topics"
        url = HOST+PORT+API_ENDPOINT
        
        r = requests.get(url)
        data = r.json()
        
        tries = 30
        while tries:
            if r.status_code == 400 and "Lock cannot be acquired." in r.text:
                r = requests.get(url)
                data = r.json()
            else:
                break
            tries -= 1
        else:
            raise MyConsumerError('Server is busy, please try again after a while.')

        if r.status_code == 200:
            topicsList = data['topics']
            return list(topicsList)
        if r.status_code == 400:
            raise MyConsumerError(data['message'])
            
    def Dequeue(self, topic, partition_id = 0) -> str:
        '''Returns the log message that is dequed from the requested topic log queue'''
        if topic not in self.__topics:
            raise MyConsumerError('Not subscribed to topic' + topic + '.')
        
        consumer_id = self.__topics[topic]

        API_ENDPOINT = "/consumer/consume"
        url = HOST+PORT+API_ENDPOINT

        if partition_id != 0:
            payload = {'topic': topic, 'consumer_id' : consumer_id, 'partition': partition_id}   

        else: 
            payload = {'topic': topic, 'consumer_id' : consumer_id}
        
        r = requests.get(url, params = payload)
        data = r.json()

        tries = 30
        while tries:
            if r.status_code == 400 and "Lock cannot be acquired." in r.text:
                r = requests.get(url, params = payload)
                data = r.json()
            else:
                break
            tries -= 1
        else:
            raise MyConsumerError('Server is busy, please try again after a while.')
            
        if r.status_code == 200:
            return data['message']
        if r.status_code == 400:
            raise MyConsumerError(data['message'])
    
    def Size(self, topic) -> int:
        '''Returns the size of log queue for the requested topic'''
        if topic not in self.__topics:
            raise MyConsumerError('Not subscribed to topic' + topic + '.')
        
        consumer_id = self.__topics[topic]

        API_ENDPOINT = "/size"
        url = HOST+PORT+API_ENDPOINT
        
        payload = {'topic': topic, 'consumer_id' : consumer_id}
        
        r = requests.get(url, params = payload)
        data = r.json()
        tries = 30
        while tries:
            if r.status_code == 400 and "Lock cannot be acquired." in r.text:
                r = requests.get(url, params = payload)
                data = r.json()
            else:
                break
            tries -= 1
        else:
            raise MyConsumerError('Server is busy, please try again after a while.')
        if r.status_code == 200:
            return int(data['size'])
        if r.status_code == 400:
            raise MyConsumerError(data['message'])