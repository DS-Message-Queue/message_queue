from src.Consumer.consumer_client import MyConsumer, MyConsumerError
import time
import threading
import os

def consume(c, c_t, index,filename):
    f = open(filename, "w")
    topics_done_count = 0
    no_message_count = {}
    for topic in c_t[index]:
        if topic == 'T-1':
            no_message_count[topic] = 1821
        elif topic == 'T-2':
            no_message_count[topic] = 2317
        elif topic == 'T-3':
            no_message_count[topic] = 827

    while topics_done_count < len(c_t[index]):
        for topic in c_t[index]:
            if no_message_count[topic] == 0:
                continue
            try:
                    text = c[index].Dequeue(topic)
                time.sleep(0.005)
                #print(text)
                f.write(text)
                no_message_count[topic] -= 1
                if no_message_count[topic] == 0:
                    topics_done_count += 1
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
        t = threading.Thread(target = consume, args = (c, c_t, i,os.getcwd() + '/tests/SystemTests/consumer_' + str(i + 1) + '.txt'))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    
    print('Consumers done')
