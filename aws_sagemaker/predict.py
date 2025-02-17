import boto3
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()

endpoint_name = os.getenv("ENDPOINT_NAME")  

runtime = boto3.client("sagemaker-runtime")

sample_input = np.array([[8.32, 0.1, 6.9, 2, 0.5, 9.3, 400, 18.5]])  

payload = ",".join(map(str, sample_input.flatten()))

response = runtime.invoke_endpoint(
    EndpointName=endpoint_name,
    ContentType="text/csv",
    Body=payload
)

result = response["Body"].read().decode("utf-8")
print(f"🎯 Previsão do modelo (preço da casa em milhares de dólares): {result}")
