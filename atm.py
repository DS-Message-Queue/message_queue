import sys
import time
from pysyncobj import SyncObj, replicated
from functools import partial

class Raft(SyncObj):
    def __init__(self, selfNodeAddr, otherNodeAddrs):
        super(Raft, self).__init__(selfNodeAddr, otherNodeAddrs)
        self.__db = {}

    @replicated(return_value=True)
    def register_user(self,id,balance):
        if id not in self.__db:
            self.__db[id] = balance
            return {"status": "success", "message": "Id is now registered."}
        return {"status": "failure", "message": "Id already exists."}

    @replicated(return_value=True)
    def withdraw(self,id,amount):
        if id not in self.__db:
            return {"status": "failure", "message": "Invalid Account Id."}
        if amount > self.__db[id]:
            return {"status": "failure", "message": "Account doesn't have enough balance."}
        self.__db[id] = self.__db[id] - amount
        return {"status": "success", "message": "Amount withdrawn."}

    @replicated(return_value=True)
    def deposit(self,id,amount):
        if id not in self.__db:
            return {"status": "failure", "message": "Invalid Account Id."}
        self.__db[id] = self.__db[id] + amount
        return {"status": "success", "message": "Amount deposited."}
    
    def inquire(self,id):
        if id not in self.__db:
            return {"status": "failure", "message": "Invalid Account Id."}
        return {"status": "success", "message": "Balance in account " + str(id) + " is " +  str(self.__db[id]) + "."}

    @replicated(return_value=True)
    def transfer(self,from_id,to_id,amount):
        if from_id not in self.__db:
            return {"status": "failure", "message": "Invalid Source Account Id."}
        if to_id not in self.__db:
            return {"status": "failure", "message": "Invalid Destination Account Id."}
        if amount > self.__db[from_id]:
            return {"status": "failure", "message": "Source Account doesn't have enough balance."}
        self.__db[from_id] = self.__db[from_id] - amount
        self.__db[to_id] = self.__db[to_id] + amount
        return {"status": "success", "message": "Transfer is successful."}
    
def onAction(res,err):
    if err:
        print(err)
    else:
        message = res["message"]
        print(message)

def register(raft:Raft):
    id = input("Please enter Account id:- ")
    try:
        id = int(id)
    except ValueError:
        print("Id can only be an positive integer value.")
        return -1
    if id < 0:
        print("Id cannot be less than 0.")
        return -1
    amount = input("Please enter amount to credit in the account:- ")
    try:
        amount = int(amount)
    except ValueError:
        print("Amount can only be an positive integer value.")
        return -1
    if amount < 0:
        print("Amount cannot be less than 0.")
        return -1
    raft.register_user(id,amount,callback=partial(onAction))
    return 0

def withdraw(raft:Raft):
    id = input("Please enter Account id:- ")
    try:
        id = int(id)
    except ValueError:
        print("Id can only be an positive integer value.")
        return -1
    if id < 0:
        print("Id cannot be less than 0.")
        return -1
    amount = input("Please enter amount to withdraw from the account:- ")
    try:
        amount = int(amount)
    except ValueError:
        print("Amount can only be an positive integer value.")
        return -1
    if amount < 0:
        print("Amount cannot be less than 0.")
        return -1
    raft.withdraw(id,amount,callback=partial(onAction))

def deposit(raft:Raft):
    id = input("Please enter Account id:- ")
    try:
        id = int(id)
    except ValueError:
        print("Id can only be an positive integer value.")
        return -1
    if id < 0:
        print("Id cannot be less than 0.")
        return -1
    amount = input("Please enter amount to deposit to the account:- ")
    try:
        amount = int(amount)
    except ValueError:
        print("Amount can only be an positive integer value.")
        return -1
    if amount < 0:
        print("Amount cannot be less than 0.")
        return -1
    raft.deposit(id,amount,callback=partial(onAction))

def inquiry(raft:Raft):
    id = input("Please enter Account id:- ")
    try:
        id = int(id)
    except ValueError:
        print("Id can only be an positive integer value.")
        return -1
    if id < 0:
        print("Id cannot be less than 0.")
        return -1
    print(raft.inquire(id)["message"])

def transfer(raft:Raft):
    from_id = input("Please enter Account id of Source Account:- ")
    try:
        from_id = int(from_id)
    except ValueError:
        print("Id can only be an positive integer value.")
        return -1
    if from_id < 0:
        print("Id cannot be less than 0.")
        return -1
    
    to_id = input("Please enter Account id of Destination Account:- ")
    try:
        to_id = int(to_id)
    except ValueError:
        print("Id can only be an positive integer value.")
        return -1
    if to_id < 0:
        print("Id cannot be less than 0.")
        return -1
    
    if from_id == to_id:
        print("Source and Destination account cannot be same.")
        return -1

    amount = input("Please enter amount to transfer from the source account:- ")
    try:
        amount = int(amount)
    except ValueError:
        print("Amount can only be an positive integer value.")
        return -1
    if amount < 0:
        print("Amount cannot be less than 0.")
        return -1

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print('Usage: %s self_port partner1_port partner2_port ...' % sys.argv[0])
        sys.exit(-1)
    port = int(sys.argv[1])
    partners = ['localhost:%d' % int(p) for p in sys.argv[2:]]
    raft = Raft('localhost:%d' % port, partners)
    leader = None
    while True:
        new_leader = raft._getLeader()
        if new_leader is None:
            print("ATM is down.")
            leader  = None
            time.sleep(1)
            continue
        if new_leader != leader:
            print("Leader is:- ",new_leader )
            leader = new_leader
        print("Select an option:-\n")
        print("\t1. Register")
        print("\t2. Withdraw")
        print("\t3. Deposit")
        print("\t4. Balance Inquiry")
        print("\t5. Transfer")
        option = input()
        option = int(option)
        if option < 1 or option > 5:
            print("Invalid Option.")
            continue
        if option == 1:
            register(raft)
        if option == 2:
            withdraw(raft)
        if option == 3:
            deposit(raft)
        if option == 4:
            inquiry(raft)
        if option == 5:
            transfer(raft)
        time.sleep(1)
