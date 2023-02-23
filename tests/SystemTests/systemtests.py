from src.Consumer.consumer_client import MyConsumer, MyConsumerError
from src.Producer.producer_client import MyProducer, MyProducerError
import time
import threading
import re
import requests
import os
import subprocess
import signal

def clear_database():
    r = requests.get(url = 'http://127.0.0.1:8002/cleardb', params = {'code' : 'xBjfq12nh'})
    if 'failure' in r.text:
        return False
    return True

def start_server():
    # stdout = None
    # subprocess.run("python3 main.py", shell=True, check=False)
    # The os.setsid() is passed in the argument preexec_fn so
    # it's run after the fork() and before  exec() to run the shell.
    pro = subprocess.Popen('python3 main.py', shell=True, preexec_fn=os.setsid)

    return os.getpgid(pro.pid)

def stop_server(gid):
    os.killpg(gid, signal.SIGTERM)  # Send the signal to all the process groups

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
def system_test_1():
    print('system test 1 executing...')

    gid = start_server()
    time.sleep(1)

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

    print('******************')
    print('Starting Producers')
    print('******************')

    threads = []
    # prducers produce
    for i in range(5):
        t = threading.Thread(target = produce, args = (p, statusList, i, os.getcwd() + '/tests/SystemTests/producer_' + str(i + 1) + '.txt'))
        t.start()
        threads.append(t)

    print('******************')
    print('Starting Consumers')
    print('******************')

    time.sleep(3)

    # consumers consume
    for i in range(3):
        t = threading.Thread(target = consume, args = (c, c_t, statusList, i,os.getcwd() + '/tests/SystemTests/consumer_' + str(i + 1) + '.txt'))
        t.start()
        threads.append(t)

    time.sleep(3)
    
    f = open('log.txt', 'w')

    # crash
    stop_server(gid)
    f.write('server crashed\n')

    # recover
    gid = start_server()
    f.write('server recovered\n')
    f.close()

    for t in threads:
        t.join()
    
    print('Producers and Consumers done')

    stop_server(gid)

def system_test_2():
    gid = start_server()

    # wait for the server to start
    time.sleep(1)

    clear_database()
    
    try:
        p = MyProducer()
        c = MyConsumer()

        p.RegisterProducer('DS')
        c.RegisterConsumer('DS')

        p.Enqueue('DS', 'message 1')

        message = c.Dequeue('DS')
    
        assert(message == 'message 1')
    
    except AssertionError:
        print('system_test_2 failed: received messages not matching sent messages')
    
    except Exception as e:
        print('system_test_2 failed:', e)
    
    finally:
        stop_server(gid)
