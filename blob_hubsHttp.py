import time
import urllib
from urllib.parse import quote_plus
import hmac
import hashlib
import base64
import requests
import json
from datetime import datetime, timezone
# from dateutil.relativedelta import relativedelta
import os
from azure.storage.blob import BlobServiceClient

# Function to generate SAS token for Event Hubs
def get_auth_token(sb_name, eh_name, sas_name, sas_value):
    uri = urllib.parse.quote_plus("https://{}.servicebus.windows.net/{}".format(sb_name, eh_name))
    sas = sas_value.encode('utf-8')
    expiry = str(int(time.time() + 3600*12))  # Token valid for 12 hours
    string_to_sign = (uri + '\n' + expiry).encode('utf-8')
    signed_hmac_sha256 = hmac.HMAC(sas, string_to_sign, hashlib.sha256)
    signature = urllib.parse.quote(base64.b64encode(signed_hmac_sha256.digest()))

    sas_token = "SharedAccessSignature sr={}&sig={}&se={}&skn={}".format(uri, signature, expiry, sas_name)
    return sas_token

# Function to send message to Event Hubs
def send_message(payload, sas_token, namespace, event_hub):
    url = f"https://{namespace}.servicebus.windows.net/{event_hub}/messages"
    hdrs = {'Content-Type': 'text/plain', 'Authorization': sas_token}

    response = requests.post(url, headers=hdrs, data=payload)
    if response.status_code == 201:
        status = "Message sent successfully."
    else:
        status = f"Failed to send message. Status code: {response.status_code}, Error: {response.text}"

    return status

# Function to extract the HL7 data from the blob content
def extract_hl7(content):
    try:
        data = json.loads(content)
        hl7_message = data.get('payload', {}).get('eventData', {}).get('HL7', '')
        return hl7_message
    except json.JSONDecodeError:
        # Handle case where content is not valid JSON
        return ''

def main():
    # Blob Storage setup
    storage_account_name = ""
    storage_account_key = ""
    container_name = ""
    prefix = ""

    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage_account_name}.blob.core.windows.net",
        credential=storage_account_key
    )
    container_client = blob_service_client.get_container_client(container_name)


    NAMESPACE = ""
    EVENT_HUB = "test-adf-1"
    SAS_NAME = "receivingtestdata"
    SAS_PRIMARY_KEY = ""

    # Generate SAS token for Event Hubs
    sas_token = get_auth_token(NAMESPACE, EVENT_HUB, SAS_NAME, SAS_PRIMARY_KEY)

    # # Time range setup
    # current_time = datetime.now(timezone.utc)
    # two_months_ago = current_time - relativedelta(months=2)

    # List blobs under the specified prefix
    blob_list = container_client.list_blobs(name_starts_with=prefix)

    for blob in blob_list:
        # Check if the blob was modified within the last two months
        # if blob.last_modified >= two_months_ago:
            # Filter for text/plain files
            if blob.content_settings.content_type == 'text/plain' or blob.name.endswith('.txt'):
                blob_client = container_client.get_blob_client(blob)

                # Read the blob content
                downloader = blob_client.download_blob()
                content = downloader.readall().decode('utf-8')  # Assuming UTF-8 encoding

                # Extract the HL7 data
                hl7_message = extract_hl7(content)

                if hl7_message:
                    # Send the HL7 message to Event Hubs
                    status = send_message(hl7_message, sas_token, NAMESPACE, EVENT_HUB)
                    print(f"Blob {blob.name}: {status}")
                else:
                    print(f"No HL7 data found in blob: {blob.name}")
            else:
                print(f"Skipping blob {blob.name} as it is not a text/plain file.")
        # else:
        #     print(f"Skipping blob {blob.name} as it is older than two months.")

if __name__ == '__main__':
    main()
