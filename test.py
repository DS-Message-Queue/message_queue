import tests.SystemTests.systemtest_producers
import tests.SystemTests.systemtest_consumers
import sys

if len(sys.argv) > 1:    
    if sys.argv[1] == 'p':
        tests.SystemTests.systemtest_producers.system_test()
    elif sys.argv[1] == 'c':
        tests.SystemTests.systemtest_consumers.system_test()