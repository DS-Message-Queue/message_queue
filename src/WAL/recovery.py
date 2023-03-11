class CrashRecovery():
    def __init__(self) -> None:
        pass
        
    def recoverLogs(self, log_type) -> list:
        '''Takes the log type as input and verifies the log file to return any unsuccessful events correspondint to that type as a list'''
        log_map = {}
        with open('WAL.log', 'r') as f:
            logs = f.readlines()
            for log in logs:
                if log_type in log:
                    words = log.strip().split(" - ")
                    txn_id = words[1]
                    if txn_id not in log_map:
                        log_map[txn_id] = ' '.join(words[3:])
                    else:
                        log_map.pop(txn_id)
        ret = []
        for k, v in log_map.items():
            ret.append((k, v))
        return ret
