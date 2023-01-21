from queue import Queue
import uuid

class MessageQueue:
    def __init__(self):
        self.topics = {}
        self.consumers ={}
        self.producers = {}

    def init(self):
        # Here we initialise the queue after fetching details from the DB.
        return 0

    # Used to add a topic in the dictionary
    def addTopic(self,topicName):
        if self.topics[topicName]:
            return -1
        myQueue = Queue(maxsize=0)
        self.topics[topicName] = myQueue
        return 0

    # To push a message to the topic queue
    def addMessageToTopic(self,topicName, message):
        if self.topics[topicName].qsize() > 0:
            self.topics[topicName].put_nowait(message)
            return 0
        return -1

    # To register a producer
    def registerProducer(self):
        self.producers[uuid.uuid1()] = {"topics": []}
        return 0

    # To register a consumer
    def registerConsumer(self):
        self.consumers[uuid.uuid1()] = {"topics": [], "position" : {}}
        return 0

    # For consumer to subscribe to a topic 
    def subscribeToTopic(self,topicName, consumerId):
        return 0

    # For producer to register as a publisher for a topic 
    def addProducerToTopic(self,topicName, producerId):
        return 0

    # For publishing a message to the Message Queue
    def publishMessage(self,topicName,message):
        return 0

    # For consuming a message from the Message Queue
    def consumeMessage(self,topicName,consumerId):
        return 0


