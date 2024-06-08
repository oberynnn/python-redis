from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer
from collections import namedtuple
from io import BytesIO
from socket import error as sock_error

class command_error(Exception): pass
class disconnect(Exception): pass

Error = namedtuple('error', ('message', ))

class protocol_handler(object):
    def handle_request(self, socket_file):
        pass
    
    def write_respones(self, socket_file, data):
        pass

class server(object):
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self.__pool = Pool(max_clients)
        self.__server = StreamServer((host, port), self.connection_handler, spawn=self.__pool)
        self.__protocol = protocol_handler()
        self.__kv = {}
    
    def connection_handler(self, connection, address):
        socket_file = connection.makefile('rwb')
        while True:
            try:
                data = self.__protocol.handle_request(socket_file)
            except disconnect:
                break
            try:
                response = self.get_response(data)
            except command_error as exc:
                response = Error(exc.args[0])
