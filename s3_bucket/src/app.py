import boto3
import os

# Initialize the S3 resource
s3 = boto3.resource('s3')

# Define the base directory
base_dir = "/home/patrick/studying_aws"

# Directory where the original files are stored
original_files_dir = os.path.join(base_dir, "s3_bucket/files/original_files")

# Directory where downloaded files will be saved
download_dir = os.path.join(base_dir, "s3_bucket/files/downloaded_files")
os.makedirs(download_dir, exist_ok=True)  # Ensure the directory exists

# S3 bucket name
bucket_name = "patotricks-bucket-example"

def upload_file(file_name: str, bucket_name: str) -> None:
    """Upload a file to an S3 bucket.

    Args:
        file_name (str): The name of the file to upload.
        bucket_name (str): The name of the S3 bucket to upload the file to.

    Returns:
        None
    """
    file_path = os.path.join(original_files_dir, file_name)
    
    try:
        with open(file_path, 'rb') as data:
            s3.Bucket(bucket_name).put_object(Key=file_name, Body=data)
        print(f"✅ File '{file_path}' uploaded to '{bucket_name}'")
    except Exception as e:
        print(f"❌ Error uploading '{file_path}': {e}")

def download_file(file_name: str, bucket_name: str, new_file_name: str) -> None:
    """Download a file from an S3 bucket.

    Args:
        file_name (str): The name of the file to download.
        bucket_name (str): The name of the S3 bucket to download the file from.
        new_file_name (str): The name to save the downloaded file as.

    Returns:
        None
    """
    download_path = os.path.join(download_dir, new_file_name)

    try:
        s3.Bucket(bucket_name).download_file(file_name, download_path)
        print(f"✅ File '{file_name}' downloaded from bucket '{bucket_name}' and saved as '{download_path}'")
    except Exception as e:
        print(f"❌ Error downloading '{file_name}': {e}")

# Loop through each file in the original files directory
for file_name in os.listdir(original_files_dir):
    print(f"📂 Processing file: {file_name}")

    # Upload the file to S3
    upload_file(file_name, bucket_name)

    # Download the file with a new name
    new_file_name = "downloaded_" + file_name
    download_file(file_name, bucket_name, new_file_name)
