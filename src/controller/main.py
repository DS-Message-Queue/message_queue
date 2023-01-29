from src.controller.utils import raise_error, raise_success
import src.Database.database as db
import threading

class Message_Queue:
    """
    The class with functions to perform actions on the Message Queue.

    Private variables:-

    1. __topics

    A dict which stores all the topics published in the Message Queue.

    Example format\n
    __topics = {
        'A' : { #'A' is the topic name.
            'producers' : [], #List of producersId which can publish to the topic.
            'consumers' : [], #List of consumersId which are subscribed to the topic.
            'messages'  : [   #List of messages which are yet to be consumed by the consumers.
                {
                    'message'     : 'Some message', #The message
                    'subscribers' : 10 #The number of consumers that can consume this message.
                }
            ],
            'bias': 10,#The number of messages that are consumed by all the subscribers
        }
    }

    2. __consumers

    A dict which stores all the consumers that are registered in the system.

    Example format\n
    __consumers = {
        '5' : { #The key is the unique id of the consumer.
            'topics' : {
                'A' : { #'A' is the topic to which consumer has subscribed to.
                    'position' : 2 #This tells from which position the consumer should consume message from.
                }
            }
        }
    }

    3. __producers

    A dict which stores all the producers that are registered in the system.

    Example format\n
    __producers = {
        '1' : { #The key is the unique id of the producer.
            'topic' : 'A' #'A' is the topic to which the producer is subscribed to.
        }
    }
    
    4. __db
    A database which contains the following schema
    topic(topic_name, bias)
    producer(p_id, topic_name)
    consumer(c_id, topic_name, position)
    message(message, topic_name, subscribers)
    """

    def __init__(self):
        """
        Constructor of the class.
        """
        self.__db = db.databases()
        self.__topics = {}
        self.__consumers = {}
        self.__producers = {}
        self.__lock = threading.Lock()
        self.init_DB()

    def init_DB(self):
        """
        This is used to initialize the class members from the persistent DB.
        """
        self.__topics, self.__producers, self.__consumers = self.__db.recover_from_crash(self.__topics, self.__producers, self.__consumers)

    def add_topic(self, topic_name: str):
        """
        Pushes a topic to the __topics member of the class.\n
        Anyone can add a topic to the system.\n
        Error handling:-

        1) If the topic already exists then throw an error.
        """
        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        # Error handling
        if topic_name in self.__topics:
            self.__lock.release()
            return raise_error("Topic " + topic_name + " already exists.")

        # Adding topic to the topics dict with no producers, no consumers and no messages.
        self.__topics[topic_name] = {
            "producers": [],
            "consumers": [],
            "messages": [],
            "bias" : 0
        }
        self.__db.insert_topic(topic_name)
        self.__lock.release()
        return raise_success("Topic " + topic_name + " added successfully.")

    # To push a message to the topic queue
    def publish_message(self, producer_id: int, topic_name: str, message: str):
        """
        Publishes a message to the provided topic.\n
        Only a producer who's registered with the topic can publish messages in it.\n
        Error handling:-

        1) If the topic doesn't exist throw an error
        2) If producer with producer_id doesn't exist then throw an error
        3) If producer hasn't registered to any topic or producer has nto registered to this topic then throw an error
        """

        # Error handling
        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        if topic_name not in self.__topics:
            self.__lock.release()
            return raise_error("Topic " + topic_name + " doesn't exist.")
        if producer_id not in self.__producers:
            self.__lock.release()
            return raise_error("Producer doesn't exist.")
        if "topic" not in self.__producers[producer_id] or self.__producers[producer_id]["topic"] != topic_name:
            self.__lock.release()
            return raise_error("Producer cannot publish to " + topic_name + ".")
        if len(self.__topics[topic_name]["consumers"]) == 0:
            self.__lock.release()
            return raise_error("No subscribers so cannot publish to the topic.")

        # What if there are no subscribers? Shall we even add the message to the topic?
        # Adding the message to the topic queue if there are subscribers otherwise not.
        if len(self.__topics[topic_name]["consumers"]) > 0:
            self.__topics[topic_name]["messages"].append({
                "message": message,
                "subscribers": len(self.__topics[topic_name]["consumers"])
            })

        self.__db.insert_for_messages(topic_name, message, len(self.__topics[topic_name]["consumers"]))
        self.__lock.release()
        return raise_success("Message " + message + " added successfully to " + topic_name + ".")

    # To register a producer
    def register_producer(self, topic_name: str):
        """
        Creates a producer in the system
        """
        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        # Generating unique Id for a producer
        producer_id = len(self.__producers) + 1
        # Adding producer to the producers dict
        self.__producers[producer_id] = {}
        if topic_name not in self.__topics:
            self.add_topic(topic_name)
        self.add_producer_to_topic(topic_name, producer_id)
        self.__db.insert_for_producer(producer_id, topic_name)
        self.__lock.release()
        return raise_success("Producer registered successfully.", {"producer_id": producer_id})

    # To register a consumer
    def register_consumer(self, topic_name : str):
        """
        Creates a register in the system
        """
        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        # Generating unique Id for a consumer
        if topic_name not in self.__topics:
                self.__lock.release()
                return raise_error("Topic doesn't exist.")
        consumer_id = len(self.__consumers) + 1
        # Adding consumer to the producers dict
        self.__consumers[consumer_id] = {"topics": {}}
        self.subscribe_to_topic(topic_name,consumer_id)
        self.__db.insert_for_consumer(consumer_id, topic_name)
        self.__lock.release()   
        return raise_success("Consumer registered successfully.", {"consumer_id": consumer_id})

    # For consumer to subscribe to a topic
    def subscribe_to_topic(self, topic_name, consumer_id):
        """
        This adds consumers under a topic.\n
        Error handling:-

        1) If the topic doesn't exist then throw an error
        2) If the consumer with consumer_id doesn't exist then throw an error
        3) If the consumer is already subscribed to the topic then throw an error
        """
        if topic_name not in self.__topics:
            return raise_error("Topic " + topic_name + " doesn't exist.")
        if consumer_id not in self.__consumers:
            return raise_error("Consumer doesn't exist.")
        if topic_name in self.__consumers[consumer_id]["topics"]:
            return raise_error("Consumer is already subscribed to the topic " + topic_name + ".")

        self.__consumers[consumer_id]["topics"][topic_name] = {
            "position": self.__topics[topic_name]["bias"] + len(self.__topics[topic_name]["messages"])
        }
        self.__topics[topic_name]["consumers"].append(consumer_id)
        return raise_success("Consumer is now subscribed to topic " + topic_name + ".")

    # For producer to register as a publisher for a topic
    def add_producer_to_topic(self, topic_name, producer_id):
        """
        This will add producer as a publisher for the given topic.\n
        Error handling:-

        1) If the topic doesn't exist then throw an error
        2) If the producer with producer_id doesn't exist then throw an error
        3) If the producer is already a publisher in another topic then throw an error
        4) If the producer is already a publisher in same topic then throw an error
        """
        if topic_name not in self.__topics:
            return raise_error("Topic " + topic_name + " doesn't exist.")
        if producer_id not in self.__producers:
            return raise_error("Invalid producer.")
        if "topic" in self.__producers[producer_id] and self.__producers[producer_id]["topic"] != topic_name:
            return raise_error("Producer cannot register to more than one topic.")
        if "topic" in self.__producers[producer_id] and self.__producers[producer_id]["topic"] == topic_name:
            return raise_error("Producer is already registered under topic " + topic_name + ".")
        self.__producers[producer_id]["topic"] = topic_name
        self.__topics[topic_name]["producers"].append(producer_id)
        return raise_success("Producer can now publish messages in topic " + topic_name + ".")

    # For consuming a message from the Message Queue

    def consume_message(self, topic_name, consumer_id):
        """
        This will send the message at the start of the queue to be consumed by the consumer.\n
        Error handling:-

        1) If the topic doesn't exist then throw an error
        2) If the consumer with consumer_id doesn't exist then throw an error
        3) If the consumer is not a subscriber of that topic then throw an error
        """
        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        if topic_name not in self.__topics:
            self.__lock.release()
            return raise_error("Topic " + topic_name + " doesn't exist.")
        if consumer_id not in self.__consumers:
            self.__lock.release()
            return raise_error("Consumer doesn't exist.")
        if topic_name not in self.__consumers[consumer_id]["topics"]:
            self.__lock.release()
            return raise_error("Consumer is not subscribed to " + topic_name + ".")
        if len(self.__topics[topic_name]["messages"]) <= 0 or self.__consumers[consumer_id]["topics"][topic_name]["position"] - self.__topics[topic_name]["bias"] >= len(self.__topics[topic_name]["messages"]):
            self.__lock.release()
            return raise_error("No new message is published to " + topic_name + ".")
        message_position = self.__consumers[consumer_id]["topics"][topic_name]["position"] - self.__topics[topic_name]["bias"]
        self.__topics[topic_name]["messages"][message_position]["subscribers"] = self.__topics[
            topic_name]["messages"][message_position]["subscribers"] - 1
        message_to_send = self.__topics[topic_name]["messages"][message_position]
        subscribers_to_send = self.__topics[topic_name]["messages"][message_position]["subscribers"]
        
        self.__db.update_for_message(self.__topics[topic_name]["messages"][message_position]["message"], subscribers_to_send)
        self.__consumers[consumer_id]["topics"][topic_name]["position"] = self.__consumers[consumer_id]["topics"][topic_name]["position"] + 1
        self.__db.update_for_consumer(consumer_id, self.__consumers[consumer_id]["topics"][topic_name]["position"])
        if message_to_send["subscribers"] == 0:
            self.__topics[topic_name]["bias"] = self.__topics[topic_name]["bias"] + 1
            self.__db.update_for_topic(topic_name, self.__topics[topic_name]["bias"])
            self.__topics[topic_name]["messages"].pop(0)
            self.__db.delete_from_message(topic_name)
            # for consumer in self.__topics[topic_name]["consumers"]:
            #     self.__consumers[consumer]["topics"][topic_name]["position"] = self.__consumers[consumer]["topics"][topic_name]["position"] - 1
        self.__lock.release()
        return raise_success("Message fetched successfully.", {
            "message": message_to_send["message"]
        })

    def list_topics(self):
        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        all_topics = list(self.__topics.keys())
        self.__lock.release()
        return raise_success("Successfully fetched topics.", {"topics": all_topics})

    def log_size(self, topic_name: str, consumer_id : int):
        isLockAvailable = self.__lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        if topic_name not in self.__topics:
            self.__lock.release()
            return raise_error("Topic " + topic_name + " doesn't exist.")
        if consumer_id not in self.__consumers:
            self.__lock.release()   
            return raise_error("Consumer doesn't exist.")
        if topic_name not in self.__consumers[consumer_id]["topics"]:
            self.__lock.release()
            return raise_error("Consumer is not subscribed to " + topic_name + ".")
        if len(self.__topics[topic_name]["messages"]) <= 0 or self.__consumers[consumer_id]["topics"][topic_name]["position"] - self.__topics[topic_name]["bias"] >= len(self.__topics[topic_name]["messages"]):
            self.__lock.release()
            return raise_success("Successfully fetched size for topic " + topic_name + ".", {"size": 0 })
        self.__lock.release()
        return raise_success("Successfully fetched size for topic " + topic_name + ".", {"size": len(self.__topics[topic_name]["messages"]) - self.__consumers[consumer_id]["topics"][topic_name]["position"] + self.__topics[topic_name]["bias"] })
