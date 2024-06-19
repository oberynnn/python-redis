from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer
from collections import namedtuple
from io import BytesIO
from socket import error as sock_error

class command_error(Exception): pass
class disconnect(Exception): pass

Error = namedtuple('error', ('message',))

class protocol_handler(object):
    def __init__(self):
        self.handlers = {
            '+': self.handle_simple_string,
            '-': self.handle_error,
            ':': self.handle_integer,
            '$': self.handle_string,
            '*': self.handle_array,
            '%': self.handle_dict
        }

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise disconnect()
        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise command_error('bad request')
        
    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip('\r\n')
    
    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip('\r\n'))
    
    def handle_integer(self, socket_file):
        return int(socket_file.readline().rstrip('\r\n'))
    
    def handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip('\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:-2]
    
    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip('\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]
    
    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))
    
    def write_respones(self, socket_file, data):
        buffer = BytesIO()
        self.__write(buffer, data)
        buffer.seek(0)
        socket_file.write(buffer.getvalue())
        socket_file.flush()

    def __write(self, buffer, data):
        if isinstance(data, str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            buffer.write('$%s\r\n%s\r\n' % (len(data), data))
        elif isinstance(data, int):
            buffer.write(':$%s\r\n' % data)
        elif isinstance(data, Error):
            buffer.write('-$%s\r\n' % data.message)

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
    
    def get_response(self, data):
        pass

    def run(self):
        self.__server.serve_forever()