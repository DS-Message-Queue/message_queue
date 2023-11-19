'''Run Broker'''
import sys
from src.broker.broker import Broker

if __name__ == "__main__":
    if len(sys.argv) > 4 and sys.argv[1] and sys.argv[2]:
        grpc_port = sys.argv[1]
        raft_port = sys.argv[2]
        other_raft_ports = sys.argv[3:]
        Broker(grpc_port, raft_port, other_raft_ports)
