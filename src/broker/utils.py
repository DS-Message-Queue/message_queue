def raise_error(message:str,data = None):
    dict1 = {"status" : 'failure',"message" : message}
    if data is None:
        return dict1
    dict1.update(data)
    return dict1

def raise_success(message:str,data = None):
    dict1 = {"status" : 'success',"message" : message}
    if data is None:
        return dict1
    dict1.update(data)
    return dict1
