import logging
import os
import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import HttpResponseError
from azure.identity import ManagedIdentityCredential
 
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP trigger function processed a request.')
 
    # Log the caller's IP address
    client_ip = req.headers.get('X-Forwarded-For')
    if client_ip:
        logging.info(f"Client IP Address: {client_ip}")
    else:
        client_ip = req.remote_addr
        logging.info(f"Client IP Address (remote): {client_ip}")
 
    # Log the Azure Function's outbound IP address
    try:
        outbound_ip_info = requests.get('https://ifconfig.me').text
        logging.info(f"Function's Outbound IP Address: {outbound_ip_info}")
    except Exception as e:
        logging.error(f"Failed to get outbound IP: {str(e)}")
 
    # Try accessing a blob in Azure Blob Storage
    try:
        blob_service_client = BlobServiceClient(account_url="https://abc.blob.core.windows.net/", credential=ManagedIdentityCredential())
        blob_client = blob_service_client.get_blob_client(container="hl7", blob="abc/cap4527.avro")
        blob_data = blob_client.download_blob().readall()
        logging.info("Blob data read successfully")
        return func.HttpResponse(f"Blob data read successfully. Function outbound IP: {outbound_ip_info}", status_code=200)
 
    except HttpResponseError as e:
        logging.error(f"Blob Storage access failed: {e.message}")
        return func.HttpResponse(f"Error accessing Blob Storage: {e.message}", status_code=500)
 
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return func.HttpResponse(f"Internal server error: {str(e)}", status_code=500)