from src.Consumer.consumer_client import MyConsumer, MyConsumerError
import time
import multiprocessing
import os

def consume(c, c_t,filename):
    f = open(filename, "w")
    message_count = {}
    for topic in c_t:
        if topic == 'T-1':
            message_count[topic] = 1821
        elif topic == 'T-2':
            message_count[topic] = 2317
        elif topic == 'T-3':
            message_count[topic] = 827
        else:
            print('invalid data')
            exit(-2)

    while len(c_t) > 0:
        topics = c_t[:]
        for topic in topics:
            try:
                text = c.Dequeue(topic)
                time.sleep(0.02)
                f.write(text)
                message_count[topic] -= 1
                if message_count[topic] == 0:
                    c_t.remove(topic)
            except MyConsumerError as e:
                print('Consumer Error: ', e)
                continue
            except Exception as e:
                print('Connection Error, retrying... : ', e)
                exit(-1)
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
        t = multiprocessing.Process(target = consume, args = (c[i], c_t[i][:], os.getcwd() + '/tests/SystemTests/consumer_' + str(i + 1) + '.txt'))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    
    print('Consumers done')
