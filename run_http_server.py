'''Run HTTP Server'''
from src.HTTPServer.http_server import MyServer

if __name__ == "__main__":
    MyServer(__name__, 'localhost', '8001', 'localhost', '50051', 'localhost', '50053')
