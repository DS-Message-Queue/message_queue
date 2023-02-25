from WAL import WAL
from recovery import *

for i in range(10):
    if i % 2 == 0:
        id = WAL.logEvent("b12_"+str(i),"regproducer_"+str(i), "123",  "foo", "A")
        WAL.logSuccess(id, "b12_"+str(i),"regproducer_"+str(i), "123",  "foo", "A")
    else:
        id = WAL.logEvent( "b12_"+str(i), "Enqueue", "123", "foo", "A", "This is message "+ str(i))
        WAL.logSuccess(id , "b12_"+str(i), "Enqueue", "123", "foo", "A", "This is message "+ str(i))


rc = CrashRecovery()

rlogs = rc.recoverLogs("b12_6")
print(rlogs)
#to clear the log file after a checkpoint
#WAL.clearlogfile()