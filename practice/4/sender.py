import socket
import json
import time
import random

HOST = 'localhost'
PORT = 12341

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(1)

print("Сервер запущено. Очікування на підключення...")

conn, addr = server_socket.accept()
print(f"Підключено до {addr}")

value_map = {
    "error": (0, 5),
    "info": (30, 40),
    "warning": (20, 25),
    "critical": (6, 20)
}

try:
    while True:
        event_type = random.choice(["error", "info", "warning", "critical"])
        (min, max) = value_map[event_type]
        data = {
            "timestamp": int(time.time() * 1000),
            "value": random.randint(min, max),
            "event_type": event_type,
        }
        message = json.dumps(data)

        conn.sendall(f"{message}\n".encode('utf-8'))
        print(f"Sent: {message}")
        time.sleep(0.01)
except KeyboardInterrupt:
    print("Server down")
finally:
    conn.close()
    server_socket.close()
