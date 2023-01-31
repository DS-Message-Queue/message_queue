from src.Consumer.consumer_client import MyConsumer
from src.Producer.producer_client import MyProducer
from src.Database.database import databases
import time
import threading
import re
import requests
import os

def clear_database():
    r = requests.get(url = 'http://127.0.0.1:8002/cleardb', params = {'code' : 'xBjfq12nh'})
    if 'failure' in r.text:
        return False
    return True

def produce(p, statusList, index, filename):
    f = open(filename, "r")
    for message in f:
        topic = re.findall('T-\d', message)
        if len(topic):
            p[index].Enqueue(topic[0], message)
            time.sleep(0.5)
    statusList[index] = True
    f.close()

def consume(c, c_t,statusList, index,filename):
    f = open(filename, "r+")
    f.truncate()
    while False in statusList:
        try:
            for topic in c_t[index]:
                text = c[index].Dequeue(topic)
                print(text)
                f.write(text)
        except:
            pass
    f.close()


# tests
def system_test_1():
    if not clear_database():
        print('failed clearing db.')
        return
    
    p:list[MyProducer] = []  
    statusList : list[bool] = []
    for i in range(5):
        p.append(MyProducer())
        statusList.append(False)
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

    p[3].RegisterProducer('T-2')

    p[4].RegisterProducer('T-2')

    # Register Consumers
    c_t = []
    c_t.append(['T-1', 'T-2', 'T-3'])
    c[0].RegisterConsumer('T-1')
    c[0].RegisterConsumer('T-2')
    c[0].RegisterConsumer('T-3')

    c_t.append(['T-1',  'T-3'])
    c[1].RegisterConsumer('T-1')
    c[1].RegisterConsumer('T-3')

    c_t.append(['T-1', 'T-3'])
    c[2].RegisterConsumer('T-1')
    c[2].RegisterConsumer('T-3')

    # prducers produce

    threads = []
    for i in range(5):
        t = threading.Thread(target = produce, args = (p, statusList, i, os.getcwd() + '/tests/SystemTests/producer_' + str(i + 1) + '.txt'))
        t.start()
        threads.append(t)

    for i in range(3):
        t = threading.Thread(target = consume, args = (c, c_t, statusList, i,os.getcwd() + '/tests/SystemTests/consumer_' + str(i + 1) + '.txt'))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()



