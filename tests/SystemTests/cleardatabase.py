import requests

def clear_database(port):
    r = requests.get(url = 'http://127.0.0.1:' + port + '/cleardb', params = {'code' : 'xBjfq12nh'})
    if 'failure' in r.text:
        return False
    return True

clear_database('8001')
clear_database('8002')
