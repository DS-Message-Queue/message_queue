from src.Consumer.consumer_client import MyConsumer, MyConsumerError
import time
import threading
import os

def consume(c, c_t,statusList, index,filename):
    done_consuming = {}
    for topic in c_t[index]:
        done_consuming[topic] = False
    f = open(filename, "w")
    while False in done_consuming.values():
        for topic in c_t[index]:
            while True:
                try:
                    text = c[index].Dequeue(topic)
                    #print(text)
                    time.sleep(0.02)
                    f.write(text)
                    break
                except MyConsumerError as e:
                    if not (False in statusList):
                        if 'No new message is published to' in str(e):
                            done_consuming[topic] = True
                        break
                except:
                    print('Connection Error, retrying...')
    f.close()

# tests
def system_test():
    print('system test 1 executing...')
    
    c:list[MyConsumer] = []
    for i in range(3):
        c.append(MyConsumer())
    statusList : list[bool] = []

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

    print('******************')
    print('Starting Consumers')
    print('******************')

    threads = []
    # consumers consume
    for i in range(3):
        t = threading.Thread(target = consume, args = (c, c_t, statusList, i,os.getcwd() + '/tests/SystemTests/consumer_' + str(i + 1) + '.txt'))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    
    print('Consumers done')
