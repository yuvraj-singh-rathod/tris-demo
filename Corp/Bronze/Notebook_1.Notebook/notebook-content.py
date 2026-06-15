# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests

# Get token from notebook context - no MFA needed
token = notebookutils.credentials.getToken('pbi')

pipeline_id = "094f7312-c157-4fd1-b115-38ebf3e5192f"

url = f"https://app.fabric.microsoft.com/pipelines/{pipeline_id}?experience=power-bi"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

body = {
    "sourceStageOrder": 0,
    "isBackwardDeployment": False,
    "options": {
        "allowOverwriteArtifact": True,
        "allowCreateArtifact": True
    }
}

response = requests.post(url, headers=headers, json=body)
print(f"Status: {response.status_code}")
print(f"Response: {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

# Get token using your credentials directly
tenant_id = "d39f97da-dbb2-40a0-9651-829e92444131"
client_id = "871c010f-5e61-4fb1-83ac-98610a7e9110"  # Power BI public client ID
username = "YOUR-EMAIL"
password = "YOUR-PASSWORD"

token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

token_response = requests.post(token_url, data={
    "grant_type": "password",
    "client_id": client_id,
    "scope": "https://analysis.windows.net/powerbi/api/.default",
    "username": username,
    "password": password
})

token = token_response.json().get("access_token")
print("Token status:", token_response.status_code)

# Now call deployment pipeline
pipeline_id = "YOUR-PIPELINE-ID"
url = f"https://api.powerbi.com/v1.0/myorg/pipelines/{pipeline_id}/deployAll"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

body = {
    "sourceStageOrder": 0,
    "isBackwardDeployment": False,
    "options": {
        "allowOverwriteArtifact": True,
        "allowCreateArtifact": True
    }
}

response = requests.post(url, headers=headers, json=body)
print(f"Status: {response.status_code}")
print(f"Response: {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
