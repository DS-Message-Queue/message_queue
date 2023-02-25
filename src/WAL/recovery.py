class CrashRecovery():
    def __init__(self) -> None:
        pass
        

    def recoverLogs(self, b_id) -> list:
        '''Takes the broker id as input and verifies the log file to return any unsuccessful events correspondint to that broker as a list'''
        bid_logs = {}
        with open('WAL.log', 'r') as f:
            logs = f.readlines()
            for log in logs:
                if str(b_id) in log:
                    txn_id = log.strip().split(" - ")[1]
                    if txn_id not in bid_logs:
                        bid_logs[txn_id] = ' '.join(log.strip().split(" - ")[3:])
                    else:
                        bid_logs.pop(txn_id)
        
        return list(bid_logs.values())
                    

