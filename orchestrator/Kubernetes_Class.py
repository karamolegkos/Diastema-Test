# Import custom Libraries
from MinIO_Class import MinIO_Class

# Import Libraries
import socket
import os
import json

class Kubernetes_Class:
    KUBERNETES_HOST = os.getenv("KUBERNETES_HOST", "localhost")
    KUBERNETES_PORT = int(os.getenv("KUBERNETES_PORT", 6006))
    K8S_HEADER = 64
    K8S_FORMAT = 'utf-8'

    def __init__(self):
        self.K8S_ADDR = (Kubernetes_Class.KUBERNETES_HOST, Kubernetes_Class.KUBERNETES_PORT)
        return
    
    def kubernetes_send(self, msg):
        socket_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_client.connect(self.K8S_ADDR)

        message = msg.encode(Kubernetes_Class.K8S_FORMAT)
        msg_length = len(message)
        send_length = str(msg_length).encode(Kubernetes_Class.K8S_FORMAT)
        send_length += b' ' * (Kubernetes_Class.K8S_HEADER - len(send_length))
        socket_client.send(send_length)
        socket_client.send(message)
        socket_client.recv(2048).decode(Kubernetes_Class.K8S_FORMAT)
        return
    
    def spark_caller(self, call_args):
        # minikube default host
        master_host = "192.168.49.2"
        # minikube default port
        master_port = "8443"

        # Probably a variable
        app_name = "diastema-job"

        # variable
        path = "local://"+call_args[0]

        # variable
        algorithm = call_args[1]

        # variable
        minio_input = call_args[2]

        # variable
        minio_output = call_args[3]

        # variable
        column = call_args[4]

        minio_obj = MinIO_Class()

        diaste_kube_json = {
            "master-host" : master_host,
            "master-port" : master_port,
            "app-name" : app_name,
            "minio-host" : minio_obj.MINIO_HOST,
            "minio-port" : str(minio_obj.MINIO_PORT),
            "minio-user" : minio_obj.MINIO_USER,
            "minio-pass" : minio_obj.MINIO_PASS,
            "path" : path,
            "algorithm" : algorithm,
            "minio-input" : minio_input,
            "minio-output" : minio_output,
            "column" : column
        }

        self.kubernetes_send(json.dumps(diaste_kube_json))
        return