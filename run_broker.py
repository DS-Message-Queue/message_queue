from src.broker.broker import *
import sys

# Added this so as to make it run in all OS
if __name__ == "__main__":
    port = "50052"
    if sys.argv[1]:
        port = sys.argv[1]
    Broker(port)
