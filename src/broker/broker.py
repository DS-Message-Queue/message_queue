import grpc
import json
from concurrent import futures
import time
import src.protos.managerservice_pb2_grpc as m_pb2_grpc
import src.protos.managerservice_pb2 as m_pb2
import src.protos.brokerservice_pb2_grpc as b_pb2_grpc
import src.protos.brokerservice_pb2 as b_pb2
from src.broker.utils import raise_error, raise_success
import multiprocessing
import threading

# Raft
from src.pysyncobjm import SyncObj, replicated, SyncObjConf
from src.pysyncobjm.transport import TCPTransport
from src.pysyncobjm.poller import createPoller
from src.pysyncobjm.node import TCPNode
from functools import partial


class ManagerConnection:
    """
    Client for gRPC functionality
    """

    def __init__(self, server_host, server_port, broker_host, broker_port, raft_port):

        # instantiate broker_id
        self.broker_id = None

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(server_host, server_port))

        # bind the client and the server
        self.stub = m_pb2_grpc.ManagerServiceStub(self.channel)

        # broker server communication channel
        self.broker_channel = grpc.insecure_channel(
            '{}:{}'.format(broker_host, broker_port))

        # bind to broker server
        self.broker_stub = b_pb2_grpc.BrokerServiceStub(self.broker_channel)

        self.raft_port = raft_port

    def health_check(self):
        """
            If manager connection is active, this return True
            Else this connects to the manager (blocking call) and then returns False
        """
        printed = False
        ret = True
        while True:
            try:
                self.stub.HealthCheck(m_pb2.HeartBeat())
            except:
                ret = False
                if not printed:
                    print('Manager disconnected, retrying...')
                    printed = True
                continue
            if printed:
                print('Manager connected.')
            return ret

    def register_broker_if_required(self, host, port, token):
        """
            Checks if manager is down and if it is, then re-register once it is up
        """
        needs_register = True
        if self.broker_id != None:  # if already registered once
            needs_register = not self.health_check()

        if needs_register:
            self.health_check()
            Status = self.stub.RegisterBroker(m_pb2.BrokerDetails(
                host=host, port=port, token=token, raft_port=self.raft_port
            ))

            if Status.status:
                print('Successfully registered.')
                self.broker_id = Status.brokerId
                self.broker_stub.ResetBroker(b_pb2.BrokerDetails(
                    brokerId=Status.brokerId
                ))


class Raft(SyncObj):
    """
        Each topic's partition is handled by a single Raft Instance
    """
    def __init__(self, transport, topic_partition, selfNodeAddr, otherNodeAddrs, conf):
        super(Raft, self).__init__(topic_partition, selfNodeAddr, otherNodeAddrs, conf, transport=transport)
        
        # init queries list, aka., topic's partition
        self.queries = []

        print('Created Raft Instance:', selfNodeAddr, ',', otherNodeAddrs)
    
    @replicated
    def append_query(self, query):
        print('appending query')
        self.queries.append(query)

    @replicated
    def clear_queries(self):
        self.queries.clear()


class BrokerService(b_pb2_grpc.BrokerServiceServicer):

    def __init__(self, raft_port, other_raft_ports):
        super().__init__()
        self.__topics = {}
        self.__producers = {}
        self.broker_id = None
        self.__publish_lock = threading.Lock()

        # all raft communications happend through this port
        self.__raft_port = raft_port

        # raft ports of all other brokers
        self.__other_raft_ports = other_raft_ports

        # create TCPNodes
        self.__selfnode = TCPNode('localhost:' + self.__raft_port)
        self.__othernodes = []
        self.__portToTCPNode = {}
        for p in self.__other_raft_ports:
            if p != self.__raft_port:
                tcpnode = TCPNode('localhost:' + p)
                self.__othernodes.append(tcpnode)
                self.__portToTCPNode[p] = tcpnode
        
        # create common objects
        self.__conf = SyncObjConf()
        self.__poller = createPoller(self.__conf.pollerType)

        # create transport object
        self.__transport = TCPTransport(self.__poller, self.__conf, self.__selfnode, self.__othernodes)

        # map to store raft instance for each topic's partition
        # self.__topic_partition_to_raft[(topic, partition)] = Raft(...)
        self.__topic_partition_to_raft = {}

        # poll for messages
        threading.Thread(target=self.poll_thread).start()

        self.__transport._onTick()

    def poll_thread(self):
        while True:
            # select command is used internally and callbacks are attached
            self.__poller.poll(0.0)

    def clear_data(self):
        for raft_instance in self.__topic_partition_to_raft.values():
            raft_instance.clear_queries()
        for topic in self.__topics:
            for partition in self.__topics[topic]:
                if partition == 'consumers' or partition == 'producers':
                    continue
                self.__topics[topic][partition]["messages"].clear()

    def ResetBroker(self, broker_details, context):
        self.broker_id = broker_details.brokerId
        self.clear_data()
        return b_pb2.Status()

    def GetUpdates(self, request, context):
        # get the queries from the partition
        temp = []
        try:
            topic_partition = (request.topic, request.partition)
            temp.extend(self.__topic_partition_to_raft[topic_partition].queries)
            self.__topic_partition_to_raft[topic_partition].clear_queries()
        except Exception as e:
            # print('GetUpdates Exception:', e)
            pass
        # Send data from here to Manager
        for query in temp:
            yield b_pb2.Query(query=query)

    def SendTransaction(self, transaction_req, context):
        transaction = json.loads(transaction_req.data)
        response = self.process_transaction(transaction)
        return b_pb2.Response(data=bytes(json.dumps(response).encode('utf-8')))

    def process_transaction(self, transaction):
        req_type = transaction['req']
        if req_type == 'Enqueue' or req_type == 'EnqueueWithPartition':
            res = self.publish_message(
                transaction["producer_id"], transaction["topic"], transaction["message"])
            return res
        elif req_type == 'CreateTopic':
            topic = transaction['topic']
            if topic not in self.__topics:
                self.__topics[topic] = {str(self.broker_id): {"messages": []}}
            return {}
        elif req_type == 'ProducerRegister':
            topic = transaction['topic']
            producer_id = transaction['producer_id']
            if topic not in self.__topics:
                self.__topics[topic] = {str(self.broker_id): {"messages": []}}
            if str(producer_id) not in self.__producers:
                self.__producers[str(producer_id)] = {"topic": topic}
            return {}
        elif req_type == 'Init':
            self.__topics = transaction['topics']
            self.__producers = transaction['producers']
            return {}
        elif req_type == 'ReplicaHandle':
            topic_partitions = transaction['topic_partitions']
            raftports = transaction['other_raftports']

            print('raft ports - ', raftports)
            
            # create Raft Instances
            print('self tcp, ', self.__portToTCPNode)
            for i in range(len(topic_partitions)):
                raftothernodes = []
                for port in raftports[i]:
                    raftothernodes.append(self.__portToTCPNode[port])
                
                topic_partition = tuple(topic_partitions[i])
                print(topic_partition)
                self.__topic_partition_to_raft[topic_partition] = Raft(self.__transport, topic_partition
                                                                       , self.__selfnode, raftothernodes, self.__conf)
            
            # for raft in self.__topic_partition_to_raft.values():
            #     new_leader = None
            #     while new_leader is None:
            #         new_leader = raft._getLeader()
            #     print('leader =', new_leader)

            return {}
        else:
            return self.add_producer(transaction["pid"], transaction["topic"])

    def add_producer(self, producer_id: int, topic_name: str):
        self.__producers[producer_id]["topic"] = topic_name

    def publish_message(self, producer_id: int, topic_name: str, message: str):
        isLockAvailable = self.__publish_lock.acquire(blocking=False)
        if isLockAvailable is False:
            return raise_error("Lock cannot be acquired.")
        if topic_name not in self.__topics:
            self.__publish_lock.release()
            return raise_error("Topic " + topic_name + " doesn't exist.")
        if str(producer_id) not in self.__producers:
            self.__publish_lock.release()
            return raise_error("Producer doesn't exist.")
        if "topic" not in self.__producers[str(producer_id)] or self.__producers[str(producer_id)]["topic"] != topic_name:
            self.__publish_lock.release()
            return raise_error("Producer cannot publish to " + topic_name + ".")
        if str(self.broker_id) in self.__topics[topic_name]:
            self.__topics[topic_name][str(self.broker_id)]["messages"].append({
                "message": message,
                "subscribers": 0
            })

        else:
            self.__topics[topic_name][str(self.broker_id)] = {
                "messages": [{
                    "message": message,
                    "subscribers": 0
                }]
            }

        query = "INSERT INTO topic(topic_name, partition_id,bias) SELECT '" + topic_name + "','" + str(self.broker_id) + \
                "', '0' WHERE NOT EXISTS (SELECT topic_name, partition_id FROM topic WHERE topic_name = '" + topic_name + \
                "' and partition_id =" + str(self.broker_id) + ");"
        # Raft append_query
        self.__topic_partition_to_raft[(topic_name, str(self.broker_id))].append_query(query)
        
        query = "INSERT INTO message(message, topic_name, partition_id, subscribers) VALUES('" + \
                message + "', '" + topic_name + "', " + str(self.broker_id) + ", " + str(0) + ");"
        # Raft append_query
        self.__topic_partition_to_raft[(topic_name, str(self.broker_id))].append_query(query)

        res = raise_success("Message added successfully.")
        self.__publish_lock.release()
        return res


class Broker:
    def __init__(self, port, raft_port, other_raft_ports):

        # retrieve broker config
        with open('./src/broker/broker.json', 'r') as config_file:
            self.config = json.load(config_file)
            self.host = self.config['host']
            self.port = port
            self.token = self.config['token']
        self.raft_port = raft_port

        # start broker service
        t = multiprocessing.Process(target=self.serve, args=[raft_port, other_raft_ports])
        t.start()

        # manager connection
        client = ManagerConnection(
            self.config['server_host'], self.config['server_port'],
            self.config['host'], self.port, self.raft_port
        )

        time.sleep(2)
        while True:
            client.register_broker_if_required(
                self.host, self.port, self.token)
            time.sleep(1)

        t.join()

    def serve(self, raft_port, other_raft_ports):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        b_pb2_grpc.add_BrokerServiceServicer_to_server(BrokerService(raft_port, other_raft_ports), server)
        ip = '{}:{}'.format(self.config['host'], self.port)
        server.add_insecure_port('[::]:' + self.port)
        print('broker listening at:', ip)
        server.start()
        server.wait_for_termination()
