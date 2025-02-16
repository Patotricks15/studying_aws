import requests
import os
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("API_ENDPOINT")

# Make a GET request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    print(data['message'])
else:
    print(f"❌ Request failed with status code: {response.status_code}")
