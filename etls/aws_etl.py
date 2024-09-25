import s3fs
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY

def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(key=AWS_ACCESS_KEY_ID,
                                secret=AWS_ACCESS_KEY,
                                anon=False)
        return s3
    except Exception as e:
        print(f'error in connecting to s3 {e}')

def create_bucket_if_not_exists(s3: s3fs.S3FileSystem, bucket_name: str):
    try:
        if not s3.exists(bucket_name):
            s3.mkdir(bucket_name)
            print(f'created bucket {bucket_name}')
        else:
            print(f'bucket {bucket_name} already exists')
    except Exception as e:
        print(f'error in creating bucket {e}')

def upload_file_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket: str, s3_file_name: str):
    try:
        s3.put(file_path, bucket, s3_file_name)
    except Exception as e:
        print(f'error in uploading file to s3 {e}')

