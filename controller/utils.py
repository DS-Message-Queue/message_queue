def raiseError(message:str,data = None):
    if data is None:
        return {"status" : 'error',"message" : message}
    return  {"status" : 'error',"message" : message,"data":data}

def raiseSuccess(message:str,data = None):
    if data is None:
        return {"status" : 'success',"message" : message}
    return {"status" : 'success',"message" : message,"data":data}
