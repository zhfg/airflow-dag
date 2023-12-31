from minio import Minio
import io


def create_minio_client(endpoint: str, access_key: str, secret_key:str, secure:bool = False):
    client = Minio(endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
    return client

def minio_update_file(
        client: Minio, 
        bucket: str, 
        src: io.BytesIO, 
        dest: str, 
        length: int, 
        content_type="application/json"
        ):
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print("Created bucket", bucket)
    else:
        print("Bucket", bucket, "already exists")

    # Upload the file, renaming it in the process
    client.put_object(
        bucket, dest, src,
        length = length,

    )
    print(
        src, "successfully uploaded as object",
        dest, "to bucket", bucket,
    )


def minio_upload_stock_list(client: Minio, bucket:str, src: str):
    dest = "all_a_stock.json"
    stocks_len = len(src)
    minio_update_file(
        client,
        bucket=bucket,
        src=io.BytesIO(src.encode('utf-8')),
        length = stocks_len,
        dest=dest,
        content_type="application/json"
    )