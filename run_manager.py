from src.manager.manager import *

# Added this so as to make it run in all OS
if __name__ == "__main__":
    Manager(__name__, 'localhost', '8002', 'localhost', '50051')
