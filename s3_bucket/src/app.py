import boto3
import os

s3 = boto3.resource('s3')


def upload_file(file_name:str, bucket_name:str) -> None:
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    """
    base_dir = "/home/patrick/studying_aws"
    original_files_dir = os.path.join(base_dir, "s3_bucket/files/original_files")

    for file in os.listdir(original_files_dir):
        file_path = os.path.join(original_files_dir, file)

        with open(file_path, 'rb') as data:
            s3.Bucket(bucket_name).put_object(Key=file, Body=data)
        print(f"File '{file_path}' sent to '{bucket_name}'")


def download_file(file_name:str, bucket_name:str, new_file_name:str) -> None:
    """Download a file from an S3 bucket

    :param file_name: File to download
    :param bucket_name: Bucket to download from
    """
    s3.Bucket(bucket_name).download_file('original_files/' +file_name, 'downloaded_file/' + new_file_name)
    print(f"File '{file_name}' downloaded from bucket '{bucket_name}' and saved as '{new_file_name}'")



bucket_name = "patotricks-bucket-example"

for i in os.listdir('s3_bucket/files/original_files'):
    print(i)
    upload_file(i, bucket_name)
    download_file(i, bucket_name, 'downloaded_' + i)