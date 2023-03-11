from src.Producer.producer_client import MyProducer, MyProducerError
import time
import threading
import re
import os

def produce(p, statusList, index, filename):
    f = open(filename, "r")
    for message in f:
        while True:
            try:
                topic = re.findall('T-\d', message)
                if len(topic) > 0:
                    p[index].Enqueue(topic[0], message)
                    time.sleep(0.02)
                break
            except MyProducerError as e:
                pass
            except:
                print('Connection Error, retrying...')

    statusList[index] = True
    f.close()

# tests
def system_test():
    print('system test executing...')
    
    p:list[MyProducer] = []  
    statusList : list[bool] = []
    for i in range(5):
        p.append(MyProducer())
        statusList.append(False)

    # Register Prodecers
    p[0].RegisterProducer('T-1')
    p[0].RegisterProducer('T-2')
    p[0].RegisterProducer('T-3')

    p[1].RegisterProducer('T-1')
    p[1].RegisterProducer('T-3')

    p[2].RegisterProducer('T-1')

    p[3].RegisterProducer('T-2')

    p[4].RegisterProducer('T-2')

    print('******************')
    print('Starting Producers')
    print('******************')

    input()

    threads = []
    # prducers produce
    for i in range(5):
        t = threading.Thread(target = produce, args = (p, statusList, i, os.getcwd() + '/tests/SystemTests/producer_' + str(i + 1) + '.txt'))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print('Producers done')
