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
    
    def write_response(self, socket_file, data):
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
        elif isinstance(data, (list, tuple)):
            buffer.write('*$%s\r\n' % len(data))
            for item in data:
                self.__write(buffer, item)
        elif isinstance(data, dict):
            buffer.write('%$%s\r\n' % len(data))
            for key in data:
                self.__write(buffer, key)
                self.__write(buffer, data[key])
        elif data == None:
            buffer.write('$-1\r\n')
        else:
            raise command_error('unrecognized data type - %s' % type(data))

class server(object):
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self.__pool = Pool(max_clients)
        self.__server = StreamServer((host, port), self.connection_handler, spawn=self.__pool)
        self.__protocol = protocol_handler()
        self.__kv = {}
        self.__commands = self.get_commands()
    
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
    
    def get_commands(self):
        return {
            'GET' : self.get,
            'SET' : self.set,
            'DELETE' : self.delete,
            'FLUSH' : self.flush,
            'MGET' : self.mget,
            'MSET' : self.mset
        }
    
    def get_response(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise command_error('request must be a list or simple string')
        if not data:
            raise command_error('command cannot be blank')
        command = data[0].upper()
        if command not in self.__commands:
            raise command_error('unrecognized command - %s' % command)
        return self.__commands[command](*data[1:])
    
    def get(self, key):
        return self.__kv.get(key)
    
    def set(self, key, value):
        self.__kv[key] = value
        return 1

    def delete(self, key):
        if key in self.__kv:
            del self.__kv[key]
            return 1
        return 0
    
    def flush(self):
        kv_len = len(self.__kv)
        self.__kv.clear()
        return kv_len
    
    def mget(self, *keys):
        return [self.__kv.get(key) for key in keys]
    
    def mset(self, *items):
        data = zip(items[::2], items[1::2])
        for key, value in data:
            self.__kv[key] = value
        return len(data)

    def run(self):
        self.__server.serve_forever()

class client(object):
    def __init__(self, host='127.0.0.1', port=31337) -> None:
        self.__protocol = protocol_handler()
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket.connect((host, port))
        self.__fh = self.__socket.makefile('rwb')

    def execute(self, *args):
        self.__protocol.write_response(self.__fh, args)
        resp = self.__protocol.handle_request(self.__fh)
        if isinstance(resp, Error):
            raise command_error(resp.message)
        return resp
    
    def get(self, key):
        return self.execute('GET', key)
    
    def set(self, key, value):
        return self.execute('SET', key, value)
    
    def delete(self, key):
        return self.execute('DELETE', key)
    
    def flush(self):
        return self.execute('FLUSH')
    
    def mget(self, *keys):
        return self.execute('MGET', *keys)
    
    def mset(self, *items):
        return self. execute('MSET', *items)
    

if __name__=="__main__":
    from gevent import monkey; monkey.patch_all()
    server().run()