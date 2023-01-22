import uuid
from controller.utils import raiseError, raiseSuccess


class MessageQueue:
    def __init__(self):
        self.topics = {}
        self.consumers = {}
        self.producers = {}

    def init(self):
        # Here we initialise the queue after fetching details from the DB.
        return 0

    # Used to add a topic in the dictionary
    def addTopic(self, topicName: str):
        if topicName in self.topics:
            return raiseError("Topic " + topicName + " already exists.")
        self.topics[topicName] = {
            "producers": [],
            "consumers": [],
            "messages": []
        }
        return raiseSuccess("Topic " + topicName + " added successfully.")

    # To push a message to the topic queue
    def publishMessage(self, producerId: str, topicName: str, message: str):
        if topicName not in self.topics:
            return raiseError("Topic " + topicName + " doesn't exist.")
        if producerId not in self.producers:
            return raiseError("Producer doesn't exist.")
        if "topic" not in self.producers[producerId] or self.producers[producerId]["topic"] != topicName:
            return raiseError("Producer cannot publish to " + topicName + ".")
        self.topics[topicName]["messages"].append({
            "message" : message,
            "subscribers" : self.topics[topicName]["consumers"].len()
        })
        return raiseSuccess("Message " + message + " added successfully to " + topicName + ".")

    # To register a producer
    def registerProducer(self):
        producerId = uuid.uuid1()
        self.producers[producerId] = {}
        return raiseSuccess("Producer registered successfully.",{"id" : producerId})

    # To register a consumer
    def registerConsumer(self):
        consumerId = uuid.uuid1()
        self.consumers[consumerId] = {"topics": {}}
        return raiseSuccess("Consumer registered successfully.",{"id" : consumerId})

    # For consumer to subscribe to a topic
    def subscribeToTopic(self, topicName, consumerId):
        if topicName not in self.topics:
            return raiseError("Topic " + topicName + " doesn't exist.")
        if consumerId not in self.consumers:
            return raiseError("Consumer doesn't exist.")
        if topicName in self.consumers[consumerId]["topics"]:
            return raiseError("Consumer is already subscribed to the topic " + topicName + ".")
        self.consumers[consumerId]["topics"][topicName] = {
            "position" : 0
        }
        return raiseSuccess("Consumer is not subscribed to topic " +  topicName + ".")

    # For producer to register as a publisher for a topic
    def addProducerToTopic(self, topicName, producerId):
        if topicName not in self.topics:
            return raiseError("Topic " + topicName + " doesn't exist.")
        if producerId not in self.producers:
            return raiseError("Invalid producer.")
        if "topic" in self.producers[producerId]:
            return raiseError("Producer cannot register to more than one topic.")
        self.producers[producerId]["topic"] = topicName
        return raiseSuccess("Producer can now publish messages in topic " + topicName + ".")


    # For consuming a message from the Message Queue
    def consumeMessage(self, topicName, consumerId):
        if topicName not in self.topics:
            return raiseError("Topic " + topicName + " doesn't exist.")
        if consumerId not in self.consumers:
            return raiseError("Consumer doesn't exist.")
        if topicName not in self.consumers[consumerId]["topics"]:
            return raiseError("Consumer is not subscribed to " + topicName + ".")
        if self.topics[topicName]["messages"].len() <= 0:
            return raiseError("No new message is published to " + topicName + ".")
        messagePosition = self.consumers[consumerId]["topics"][topicName]["position"]
        messageToSend = self.topics[topicName]["messages"][messagePosition]
        if messageToSend["subscribers"] == 1:
            self.topics[topicName]["messages"].pop(0)
            # Decrease the position by 1 for all subscribers of the topic
        else:
            self.consumers[consumerId]["topics"][topicName]["position"] = messagePosition + 1
        return raiseSuccess("Message fetched successfully.",{
            "message" : messageToSend["message"]
        })
    