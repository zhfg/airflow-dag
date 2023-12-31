from minio import Minio


def create_minio_client(endpoint: str, access_key: str, secret_key:str, secure:bool = False):
    client = Minio(endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
    return client

def minio_update_file(client: Minio, bucket: str, src: bytes, dest: str):
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print("Created bucket", bucket)
    else:
        print("Bucket", bucket, "already exists")

    # Upload the file, renaming it in the process
    client.put_object(
        bucket, dest, src,
    )
    print(
        src, "successfully uploaded as object",
        dest, "to bucket", bucket,
    )