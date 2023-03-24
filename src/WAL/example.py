from WAL import WriteAheadLog
from recovery import *

wal = WriteAheadLog()

# To clear the log file after a checkpoint
wal.clearlogfile()

for i in range(10):
    if i % 2 == 0:
        id = wal.logEvent("query" , "INSERT into message...")
        wal.logSuccess(id, "query")
    else:
        id = wal.logEvent("topic", "INSERT into topic...")
        # wal.logSuccess(id, "topic")

rc = CrashRecovery()
qlogs = rc.recoverLogs("query")
tlogs = rc.recoverLogs("topic")
print(qlogs)
print(tlogs)
