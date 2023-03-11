from src.Consumer.consumer_client import MyConsumer, MyConsumerError
import time
import threading
import os

def consume(c, c_t, index,filename):
    f = open(filename, "w")
    count = 993*5
    while True:
        if count == 0:
            break
        count -= 1
        for topic in c_t[index]:
            while True:
                try:
                    text = c[index].Dequeue(topic)
                    #print(text)
                    time.sleep(0.02)
                    f.write(text)
                    break
                except MyConsumerError as e:
                    print('Consumer Error: ', e)
                    break
                except Exception as e:
                    print('Connection Error, retrying... : ', e)
    f.close()

# tests
def system_test():
    print('system test 1 executing...')
    
    c:list[MyConsumer] = []
    for i in range(3):
        c.append(MyConsumer())

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

    input()
    
    threads = []
    # consumers consume
    for i in range(3):
        t = threading.Thread(target = consume, args = (c, c_t, i,os.getcwd() + '/tests/SystemTests/consumer_' + str(i + 1) + '.txt'))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    
    print('Consumers done')
