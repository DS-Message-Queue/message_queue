import requests

HOST = "http://127.0.0.1:"
PORT = "8002"

class myConsumerError(Exception):
    '''Error in consumer'''

class myConsumer():
    

    def __init__(self, consumerId = None):
        #if the consumerId is already known, the consumer can directly access the topics and logs
        self.consumerId = consumerId
        

    def RegisterConsumer(self, topic):
        '''Registers the consumer to a topic and assign the topic specific consuerId
            Inorder to subscribe to multiple topics call this function multiple times'''
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
            raise myConsumerError('Server is busy, please try again after a while.')
        if r.status_code == 200:
            self.consumerId = data['consumer_id']                #assign the consumerId returned by the server
            return self.consumerId
        if r.status_code == 400:
            raise myConsumerError(data['message'])
        
            
    
    def ListTopics(self):
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
            raise myConsumerError('Server is busy, please try again after a while.')

        if r.status_code == 200:
            topicsList = data['topics']
            return topicsList
        if r.status_code == 400:
            raise myConsumerError(data['message'])
            
    def Dequeue(self, topic, consumerId):
        '''Returns the log message that is dequed form the requested topic log queue'''
        self.consumerId = consumerId
        API_ENDPOINT = "/consumer/consume"
        url = HOST+PORT+API_ENDPOINT
        payload = {'topic': topic, 'consumer_id' : self.consumerId}
        
        r = requests.get(url, params = payload)
        data = r.json()

        tries = 30
        while tries:
            if r.status_code == 400 and "Lock cannot be acquired." in r.text:
                r = requests.post(url, json = payload)
                data = r.json()
            else:
                break
            tries -= 1
        else:
            raise myConsumerError('Server is busy, please try again after a while.')
            
        if r.status_code == 200:
            return data['message']
        if r.status_code == 400:
            raise myConsumerError(data['message'])
                
            

    def Size(self, topic, consumerId):
        '''Returns the size of log queue for the requested topic'''
        self.consumerId = consumerId
        API_ENDPOINT = "/size"
        url = HOST+PORT+API_ENDPOINT
        
        payload = {'topic': topic, 'consumer_id' : self.consumerId}

        
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
            raise myConsumerError('Server is busy, please try again after a while.')
        if r.status_code == 200:
            return data['size']
        if r.status_code == 400:
            raise myConsumerError(data['message'])


# consumer = myConsumer()
# #print(consumer.RegisterConsumer('example_topic'))
# #print(consumer.ListTopics())
# #print(consumer.Size('example_topic', 7))
# print(consumer.Dequeue('example_topic', 7))