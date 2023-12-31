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

def minio_get_object(
        client: Minio, 
        bucket: str, 
        object_name,
        ):
    found = client.bucket_exists(bucket)
    if not found:
        return None
    else:
        return client.get_object(
            bucket, object_name
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

def minio_get_stock_list(client: Minio, bucket:str):
    dest = "all_a_stock.json"
    return minio_get_object(
        client, bucket, dest,
    ).json()

def minio_upload_daily_kline(client: Minio, bucket: str, src: str, market: int, code: str):
    dest = "kline_daily_{}.{}.json".format(market, code)
    stocks_len = len(src)
    minio_update_file(
        client,
        bucket=bucket,
        src=io.BytesIO(src.encode('utf-8')),
        length = stocks_len,
        dest=dest,
        content_type="application/json"
    )

def minio_get_stock_kline(client: Minio, bucket:str, market: int, code: str):
    dest = "kline_daily_{}.{}.json".format(market, code)
    return minio_get_object(
        client, bucket, dest,
    ).json()