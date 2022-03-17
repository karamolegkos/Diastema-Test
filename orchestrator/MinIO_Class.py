import os
from minio import Minio

class MinIO_Class:
    MINIO_HOST = os.getenv("MINIO_HOST", "localhost")
    MINIO_PORT = int(os.getenv("MINIO_PORT", 9000))
    MINIO_USER = os.getenv("MINIO_USER", "diastema")
    MINIO_PASS = os.getenv("MINIO_PASS", "diastema")

    def __init__(self):
        minio_host = MinIO_Class.MINIO_HOST+":"+str(MinIO_Class.MINIO_PORT)
        self.minio_client = Minio(
            minio_host,
            access_key=MinIO_Class.MINIO_USER,
            secret_key=MinIO_Class.MINIO_PASS,
            secure=False
        )
        return
    
    def put_object(self, bucket, object_as_path, bytes_input, size):
        self.minio_client.put_object(bucket, object_as_path, bytes_input, size)
        return

    def remove_object(self, bucket, object_as_path):
        self.minio_client.remove_object(bucket, object_as_path)
        return
    
    def make_bucket(self, bucket):
        if self.minio_client.bucket_exists(bucket):
            pass
        else:
            self.minio_client.make_bucket(bucket)
        return

