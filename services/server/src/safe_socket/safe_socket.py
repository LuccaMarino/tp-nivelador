import socket



def recv_all(socket: socket.socket, size):
    data = b""
    while len(data) < size:
        chunk = socket.recv(size - len(data))
        if not chunk:
            if not data:
                return b""
            raise ConnectionError("[recv] connection closed before receiving all data")
        data += chunk
    return data


def send_all(socket: socket.socket, bytes):
    total_sent = 0
    while total_sent < len(bytes):
        sent = socket.send(bytes[total_sent:])
        total_sent += sent
