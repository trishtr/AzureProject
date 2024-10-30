from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import ManagedIdentityCredential
import json
# from datetime import datetime, timezone
# from dateutil.relativedelta import relativedelta

# Replace with your storage account name and container name
storage_account_name = ''
container_name = ''

# Replace with your Event Hub namespace and name
event_hub_namespace = ''
event_hub_name = ''

# Authenticate using ManagedIdentityCredential
credential = ManagedIdentityCredential()

# Use standard service URLs or private endpoint URLs based on your setup
use_privatelink = True  # Set to True if using private endpoints

if use_privatelink:
    # Use private endpoint URLs
    blob_account_url = f"https://{storage_account_name}.privatelink.blob.core.windows.net"
    event_hub_namespace_url = f"{event_hub_namespace}.privatelink.servicebus.windows.net"
else:
    # Use standard service URLs
    blob_account_url = f"https://{storage_account_name}.blob.core.windows.net"
    event_hub_namespace_url = f"{event_hub_namespace}.servicebus.windows.net"

# Connect to Blob Storage
blob_service_client = BlobServiceClient(
    account_url=blob_account_url,
    credential=credential
)
container_client = blob_service_client.get_container_client(container_name)

# Connect to Event Hubs
eventhub_producer_client = EventHubProducerClient(
    fully_qualified_namespace=event_hub_namespace_url,
    eventhub_name=event_hub_name,
    credential=credential
)

# Define the prefix and time range
prefix = f'AP/2024/7/'
# current_time = datetime.now(timezone.utc)
# two_months_ago = current_time - relativedelta(months=2)

# Function to extract the HL7 data from the blob content
def extract_hl7(content):
    try:
        data = json.loads(content)
        hl7_message = data.get('payload', {}).get('eventData', {}).get('HL7', '')
        print(hl7_message)
        return hl7_message
    except json.JSONDecodeError:
        # Handle case where content is not valid JSON
        return ''

# List and read text/plain files from Blob Storage and send extracted HL7 data to Event Hubs
with eventhub_producer_client:
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
                content = downloader.readall().decode('utf-8')  

                # Extract the HL7 data
                hl7_message = extract_hl7(content).encode('unicode_escape')

                if hl7_message:
                    # Create an EventData object with the HL7 message
                    event_data = EventData(hl7_message)
                   
                    # Send the event data
                    eventhub_producer_client.send_event(event_data)
                else:
                    print(f"No HL7 data found in blob: {blob.name}")
        # else:
        #     print(f"Skipping blob {blob.name} as it is older than two months.")
