import boto3
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()

endpoint_name = os.getenv("ENDPOINT_NAME")  

# Create a SageMaker runtime client
runtime = boto3.client("sagemaker-runtime")

# Read the input values
sample_input = np.array([[8.32, 0.1, 6.9, 2, 0.5, 9.3, 400, 18.5]])  

payload = ",".join(map(str, sample_input.flatten()))

# Invoke the endpoint
response = runtime.invoke_endpoint(
    EndpointName=endpoint_name,
    ContentType="text/csv",
    Body=payload
)

# Print the result
result = response["Body"].read().decode("utf-8")
print(f"🎯 Previsão do modelo (preço da casa em milhares de dólares): {result}")
