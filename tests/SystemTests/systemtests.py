from src.Consumer.consumer_client import MyConsumer
from src.Producer.producer_client import MyProducer
from src.Database.database import databases
import threading

def clear_database():
    db = databases()
    db.clear_database()

def producer_produce(p, index, filename):
    f = open(filename, "r")
    for message in f:
        p[index].Enqueue('')

# tests
def system_test_1():
    clear_database()
    
    p:list[MyProducer] = []  
    for i in range(5):
        p.append(MyProducer())
    c:list[MyConsumer] = []
    for i in range(3):
        c.append(MyConsumer())

    # Register Prodecers
    p[0].RegisterProducer('T-1')
    p[0].RegisterProducer('T-2')
    p[0].RegisterProducer('T-3')

    p[1].RegisterProducer('T-1')
    p[1].RegisterProducer('T-3')

    p[2].RegisterProducer('T-1')

    p[1].RegisterProducer('T-2')

    p[1].RegisterProducer('T-2')

    # Register Consumers
    c[0].RegisterConsumer('T-1')
    c[0].RegisterConsumer('T-2')
    c[0].RegisterConsumer('T-3')

    c[1].RegisterConsumer('T-1')
    c[1].RegisterConsumer('T-3')

    c[2].RegisterConsumer('T-1')
    c[2].RegisterConsumer('T-3')

    # prducers produce

producer_produce(1, 'test.txt')





