import boto3
import pandas as pd
import os
from io import StringIO

# Configure the S3 client
s3 = boto3.client('s3')

# S3 bucket name (set this variable in Lambda or your local environment)
BUCKET_NAME = os.environ.get('S3_BUCKET', 'my-example-bucket')

# Directory where files will be stored
LOCAL_DIR = "files/"
os.makedirs(LOCAL_DIR, exist_ok=True)  # Create the folder if it does not exist

# File names
FILE_NAME = "iris_dataset.csv"
LOCAL_FILE_PATH = os.path.join(LOCAL_DIR, FILE_NAME)


def collect_data():
    """
    Downloads the Iris dataset, adds new columns, and saves it as a CSV in the 'files/' directory.
    """
    try:
        # Download the dataset
        df = pd.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv")

        # Create new columns
        df['sepal_ratio'] = df['sepal_length'] / df['sepal_width']
        df['petal_ratio'] = df['petal_length'] / df['petal_width']

        # Save as CSV locally in the 'files/' directory
        df.to_csv(LOCAL_FILE_PATH, index=False)
        print(f"✅ Dataset saved locally: {LOCAL_FILE_PATH}")
        return LOCAL_FILE_PATH

    except Exception as e:
        print(f"❌ Error collecting data: {e}")
        return None


def upload_data():
    """
    Uploads the CSV file to S3.
    """
    try:
        if not os.path.exists(LOCAL_FILE_PATH):
            print("❌ Local file not found. Generating the dataset first...")
            collect_data()

        # Upload to S3
        s3.upload_file(LOCAL_FILE_PATH, BUCKET_NAME, FILE_NAME)
        print(f"✅ File '{FILE_NAME}' uploaded to S3 bucket '{BUCKET_NAME}'")

    except Exception as e:
        print(f"❌ Error uploading to S3: {e}")


def download_data():
    """
    Downloads the CSV file from S3 and saves it in the 'files/' directory.
    """
    try:
        download_path = os.path.join(LOCAL_DIR, "downloaded_" + FILE_NAME)  # Name of the downloaded file

        # Download from S3 and save in the 'files/' directory
        s3.download_file(BUCKET_NAME, FILE_NAME, download_path)
        print(f"✅ File '{FILE_NAME}' downloaded from S3 and saved as '{download_path}'")
        return download_path

    except Exception as e:
        print(f"❌ Error downloading file from S3: {e}")
        return None


# Test functions (For local execution)
if __name__ == "__main__":
    collect_data()
    upload_data()
    download_data()
