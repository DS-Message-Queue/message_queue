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
        # Error handling
        if topicName in self.topics:
            return raiseError("Topic " + topicName + " already exists.")

        # Adding topic to the topics dict with no producers, no consumers and no messages.
        self.topics[topicName] = {
            "producers": [],
            "consumers": [],
            "messages": []
        }
        return raiseSuccess("Topic " + topicName + " added successfully.")

    # To push a message to the topic queue
    def publishMessage(self, producerId: str, topicName: str, message: str):
        # Error handling
        if topicName not in self.topics:
            return raiseError("Topic " + topicName + " doesn't exist.")
        if producerId not in self.producers:
            return raiseError("Producer doesn't exist.")
        if "topic" not in self.producers[producerId] or self.producers[producerId]["topic"] != topicName:
            return raiseError("Producer cannot publish to " + topicName + ".")
        
        # What if there are no subscribers? Shall we even add the message to the topic?
        # Adding the message to the topic queue if there are subscribers otherwise not.
        if len(self.topics[topicName]["consumers"]) > 0:
            self.topics[topicName]["messages"].append({
                "message": message,
                "subscribers": len(self.topics[topicName]["consumers"])
            })
        return raiseSuccess("Message " + message + " added successfully to " + topicName + ".")

    # To register a producer
    def registerProducer(self):
        # Generating unique Id for a producer
        producerId = str(uuid.uuid1())
        # Adding producer to the producers dict
        self.producers[producerId] = {}
        return raiseSuccess("Producer registered successfully.", {"id": producerId})

    # To register a consumer
    def registerConsumer(self):
        # Generating unique Id for a consumer
        consumerId = str(uuid.uuid1())
        # Adding consumer to the producers dict
        self.consumers[consumerId] = {"topics": {}}
        return raiseSuccess("Consumer registered successfully.", {"id": consumerId})

    # For consumer to subscribe to a topic
    def subscribeToTopic(self, topicName, consumerId):
        if topicName not in self.topics:
            return raiseError("Topic " + topicName + " doesn't exist.")
        if consumerId not in self.consumers:
            return raiseError("Consumer doesn't exist.")
        if topicName in self.consumers[consumerId]["topics"]:
            return raiseError("Consumer is already subscribed to the topic " + topicName + ".")
        self.consumers[consumerId]["topics"][topicName] = {
            "position": 0
        }
        self.topics[topicName]["consumers"].append(consumerId)
        return raiseSuccess("Consumer is now subscribed to topic " + topicName + ".")

    # For producer to register as a publisher for a topic
    def addProducerToTopic(self, topicName, producerId):
        if topicName not in self.topics:
            return raiseError("Topic " + topicName + " doesn't exist.")
        if producerId not in self.producers:
            return raiseError("Invalid producer.")
        if "topic" in self.producers[producerId] and self.producers[producerId]["topic"] != topicName:
            return raiseError("Producer cannot register to more than one topic.")
        if "topic" in self.producers[producerId] and self.producers[producerId]["topic"] == topicName:
            return raiseError("Producer is already registered under topic " + topicName + ".")
        self.producers[producerId]["topic"] = topicName
        self.topics[topicName]["producers"].append(producerId)
        return raiseSuccess("Producer can now publish messages in topic " + topicName + ".")

    # For consuming a message from the Message Queue

    def consumeMessage(self, topicName, consumerId):
        if topicName not in self.topics:
            return raiseError("Topic " + topicName + " doesn't exist.")
        if consumerId not in self.consumers:
            return raiseError("Consumer doesn't exist.")
        if topicName not in self.consumers[consumerId]["topics"]:
            return raiseError("Consumer is not subscribed to " + topicName + ".")
        if len(self.topics[topicName]["messages"]) <= 0 or self.consumers[consumerId]["topics"][topicName]["position"] >= len(self.topics[topicName]["messages"]):
            return raiseError("No new message is published to " + topicName + ".")
        messagePosition = self.consumers[consumerId]["topics"][topicName]["position"]
        self.topics[topicName]["messages"][messagePosition]["subscribers"] = self.topics[topicName]["messages"][messagePosition]["subscribers"] - 1
        messageToSend = self.topics[topicName]["messages"][messagePosition]
        self.consumers[consumerId]["topics"][topicName]["position"] = messagePosition + 1
        if messageToSend["subscribers"] == 0:
            self.topics[topicName]["messages"].pop(0)
            for consumer in self.topics[topicName]["consumers"]:
                self.consumers[consumer]["topics"][topicName]["position"] = self.consumers[consumer]["topics"][topicName]["position"] - 1
        return raiseSuccess("Message fetched successfully.", {
            "message": messageToSend["message"]
        })
