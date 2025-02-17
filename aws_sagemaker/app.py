import boto3
import sagemaker
import pandas as pd
import os
from sagemaker.inputs import TrainingInput
from sklearn.datasets import fetch_california_housing
import os
from dotenv import load_dotenv

load_dotenv()

housing = fetch_california_housing(as_frame=True)
df = housing.data
df["target"] = housing.target 

train_data = df.sample(frac=0.8, random_state=42)
test_data = df.drop(train_data.index)

os.makedirs("data", exist_ok=True)
train_data.to_csv("data/train.csv", index=False, header=False)
test_data.to_csv("data/test.csv", index=False, header=False)

session = sagemaker.Session()
role = os.getenv("GET_ROLE")
bucket = session.default_bucket()
prefix = "sagemaker-xgboost-regression"

s3_train_path = session.upload_data(path="data/train.csv", bucket=bucket, key_prefix=prefix)
s3_test_path = session.upload_data(path="data/test.csv", bucket=bucket, key_prefix=prefix)
print(f"Dados enviados para S3: {s3_train_path}")

xgb_container = sagemaker.image_uris.retrieve("xgboost", boto3.Session().region_name, "1.5-1")

xgb_estimator = sagemaker.estimator.Estimator(
    image_uri=xgb_container,
    role=role,
    instance_count=1,
    instance_type="ml.m5.large",
    output_path=f"s3://{bucket}/{prefix}/output",
    sagemaker_session=session
)

xgb_estimator.set_hyperparameters(
    objective="reg:squarederror",
    num_round=20
)

train_input = TrainingInput(s3_train_path, content_type="csv")

xgb_estimator.fit({"train": train_input})
print("Modelo de regressão treinado com sucesso!")

predictor = xgb_estimator.deploy(
    initial_instance_count=1,
    instance_type="ml.m5.large"
)
print(f"Modelo implantado no endpoint: {predictor.endpoint_name}")

