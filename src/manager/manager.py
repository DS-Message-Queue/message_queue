'''Manager'''
import threading
from queue import Queue
from concurrent import futures
import json
import grpc
import src.protos.managerservice_pb2_grpc as pb2_grpc
import src.protos.managerservice_pb2 as pb2
import src.Database.main_db as db
from src.MessageQueue.main import MessageQueue


class ManagerService(pb2_grpc.ManagerServiceServicer):

    def __init__(self, mq:Queue, bq:Queue, rq:Queue) -> None:
        super().__init__()

        # shared objects
        self.mq = mq
        self.bq = bq
        self.rq = rq

        # message_queue replicated objects
        self.__lock = threading.Lock()
        self.__topics = {}
        self.__producers = {}
        self.__topics, self.__producers, _ = db.databases().recover_from_crash(
            self.__topics, self.__producers, [])

    def RegisterBroker(self, broker, context):
        print('register broker requested')
        self.bq.put(broker)
        return pb2.Status(status=True)

    def RegisterReplica(self, replica_details, context):
        print('manager replica register requested')
        self.rq.put(replica_details)

        # if token is valid:
        return pb2.Response(
            status=True,
            replicaId=1   # future work
        )

    def HealthCheck(self, heartbeat, context):
        return heartbeat

    def SendTransaction(self, transaction_req, context):
        transaction = json.loads(transaction_req.data)
        output = {}
        transaction_type = transaction['req']

        if transaction_type == 'ClearDatabase':
            print('cleardb requested')
            with self.__lock:
                try:
                    db.databases().clear_database()
                    self.__topics = {}
                    self.__producers = {}
                    output = {"status": "success",
                              "message": "Database Cleared successfully."}
                except Exception:
                    output = {"status": "failure",
                              "message": "Couldn't clear database."}
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'GetTopics':
            with self.__lock:
                output = {"status": "success", "topics": self.__topics}
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'GetPartition':
            topic_requested = transaction['topic']
            with self.__lock:
                if topic_requested not in self.__topics:
                    output = {"status": "failure", "message": "Invalid Topic."}
                else:
                    partitions = self.__topics[topic_requested][:]
                    output = {"status": "success", "partitions": partitions,
                              "number_of_partitions": len(partitions)}
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'CreateTopic':
            topic_requested = transaction['topic']
            with self.__lock:
                if topic_requested in self.__topics:
                    output = {"status": "failure",
                              "message": "Topic already exists."}
                else:
                    self.mq.put(transaction)
                    self.__topics[topic_requested] = {1: {"messages": []}}
                    output = {"status": "success",
                              "message": "Topic created."}
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'ProducerRegister':
            topic_requested = transaction['topic']
            with self.__lock:
                if topic_requested not in self.__topics:
                    self.mq.put(transaction)
                    self.__topics[topic_requested] = {1: {"messages": []}}
                producer_id = len(self.__producers) + 1
                transaction["producer_id"] = producer_id
                self.mq.put(transaction)
                self.__producers[producer_id] = {'topic': topic_requested}
                output = {"status": "success",
                          "message": "Producer created successfully.", "producer_id": producer_id}
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'Enqueue':
            with self.__lock:
                self.mq.put(transaction)
                output = {"status": "success",
                        "message": "Enqueue request successfully submitted."}
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        elif transaction_type == 'EnqueueWithPartition':
            topic_requested = transaction['topic']
            partition = transaction['partition']
            with self.__lock:
                if partition not in self.__topics[topic_requested]:
                    output = {"status": "failure",
                            "message": "partition does not exist."}
                else:
                    self.mq.put(transaction)
                    output = {"status": "success",
                            "message": "Enqueue in partition request successfully submitted."}
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

        else:
            output = {"status": "failure", "message": "Invalid Operation"}
            return pb2.TransactionResponse(data=json.dumps(output).encode('utf-8'))

class Manager:
    def __init__(self):

        # shared objects
        self.mq = Queue()
        self.bq = Queue()
        self.rq = Queue()

        # start server
        server_thread = threading.Thread(target=self.serve_grpc, args=[self.mq, self.bq, self.rq])
        server_thread.start()

        # start worker
        message_queue = MessageQueue(self.mq, self.bq, self.rq)
        message_queue.worker()

    def serve_grpc(self, mq:Queue, bq:Queue, rq:Queue):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        pb2_grpc.add_ManagerServiceServicer_to_server(ManagerService(mq, bq, rq), server)
        server.add_insecure_port('[::]:50051')
        print('manager listening at:', 'localhost:50051')
        server.start()
        server.wait_for_termination()
