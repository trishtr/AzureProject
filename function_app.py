import logging
import json
import os
from io import BytesIO
from datetime import datetime, timezone
from hashlib import sha256
# ---
import azure.functions as func
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient
# ---
from fastavro import reader
from hl7parser.hl7 import HL7Message
from hl7apy.parser import parse_message

# Map of Unsupported HL7 Version to Supported Version
INVALID_VERSION_MAP = {
    '2.0': '2.5.1',
    '2.1': '2.5.1', 
    '2.7.1': '2.8', 
    '2.9': '2.8', 
    '2.9.1': '2.8'
}

# Input Arguments - Tags/Keys
ARG_FROM_SOURCE_FILE_AVRO = 'from_source_file_avro'
ARG_INTERFACE_CONFIG = 'interface_config'
ARG_HL7_CLIENT_CONFIG = 'hl7_client_config'

# Globals/Constants
LOG_MESSAGE_CD_OK = 'COMPLETED'

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route='http_trigger_parsehl7')
def http_trigger_parsehl7(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # 1. read body and inputs from the request
        req_body = req.get_json()
        fsfavro = req_body.get(ARG_FROM_SOURCE_FILE_AVRO)
        ic = req_body.get(ARG_INTERFACE_CONFIG)
        hl7cc = req_body.get(ARG_HL7_CLIENT_CONFIG)

        # 2. check if inputs are null, raise attributeerror if so
        if not all([fsfavro, ic, hl7cc]):
            raise AttributeError(f"NULL_INPUT - Input Parameter/Value is Null - {ARG_INTERFACE_CONFIG}, {ARG_HL7_CLIENT_CONFIG}")

        # 3. read avro file speficied in interface_config & parse each hl7 message (using managed identity)
        blob_service_client = BlobServiceClient(account_url=ic.get('from_source_host'), credential=ManagedIdentityCredential())
        container_client = blob_service_client.get_container_client(ic.get('from_source_container'))
        avro_file = container_client.get_blob_client(f"{ic.get('from_source_folder')}/{fsfavro}")
        avro_data = BytesIO(avro_file.download_blob().readall())

        # 4. read avro and process each hl7 message in body
        avro_reader = reader(avro_data)
        for record in avro_reader:
            message = get_message(record.get('Body'), ic, hl7cc, context.function_name)

            # write message/output to parsed/error location
            out_file_stem = os.path.splitext(os.path.basename(fsfavro))[0]
            out_file_name = out_file_stem + '_' + message.get('message_uid') + '.json'
            out_file = container_client.get_blob_client(f"{ic.get('to_source_folder')}/{out_file_name}")          
            out_file.upload_blob(json.dumps(message), overwrite=True)
        
        # 5. move processed avro to capture archive folder
        archive_avro_file = container_client.get_blob_client(f"{ic.get('from_source_archive_folder')}/{fsfavro}")
        copy_opr = archive_avro_file.start_copy_from_url(source_url=avro_file.url, requires_sync=True)
        if copy_opr.get('copy_status') == 'success':
            avro_file.delete_blob()

    except (ValueError, AttributeError, TypeError)  as e:
        err_msg = f"INVALID_INPUT - Input Parameter/Value is Invalid - {type(e).__name__} - {str(e)}"
        logging.error(err_msg)
        return func.HttpResponse(err_msg, status_code=400)
    except Exception as e:
        err_msg = f"UNKNOWN - Unknown/Undefined Error - error occurred - {type(e).__name__} - {str(e)}"
        logging.error(err_msg)
        return func.HttpResponse(err_msg, status_code=500)

    return func.HttpResponse(f"{LOG_MESSAGE_CD_OK} - Process/Routine/Function Completed Successfully", status_code=200)

def get_message(hl7_message, ic, hl7cc, function_name):
    # process each hl7_message and return json with parsed values OR error

    message_uid = sha256(hl7_message).hexdigest()
    orig_hl7_message = hl7_message.decode('utf-8')

    # initialize return message
    message = {
        'message_uid': message_uid, 
        'processed_dttm': datetime.now(timezone.utc).isoformat(),
        'interface_id': ic.get('interface_id'),
        'interface_short_name': ic.get('short_name'),
        'from_source_id': ic.get('from_source_id'),
        'from_source_short_name': ic.get('from_source_short_name'),
        'to_source_id': ic.get('to_source_id'),
        'to_source_short_name': ic.get('to_source_short_name'),
        'hl7_raw': orig_hl7_message,
        'hl7_parsed': None  ,
        'logged_by': function_name,
        'log_severity_cd': 'INFO',
        'log_message_cd': LOG_MESSAGE_CD_OK,
        'more_info': None
    }

    try:
        # check version and override unsupported - use hl7parser
        pm1 = HL7Message(orig_hl7_message)
        vid = pm1.msh.version_id
        str_vid = str(vid[0])

        if str_vid in INVALID_VERSION_MAP:
            vid.set_attributes(vid.field_map, [INVALID_VERSION_MAP.get(str_vid)])
            ovrd_hl7_message = '\r'.join(str(pm1).splitlines())
        else:
            ovrd_hl7_message = orig_hl7_message

        # parse message using hl7apy
        pm2 = parse_message(ovrd_hl7_message)
        hl7_parsed = {'message_uid': message_uid, 'original_version_id': str_vid}

        for hl7_key, label in hl7cc.items():
            hl7_key_parts = hl7_key.split('.')
            hl7_object = pm2
            value = ""

            for part in hl7_key_parts:
                hl7_object = getattr(hl7_object, part, None)
                if hl7_object is None:
                    break
                else:
                    value = hl7_object.value.strip()
            
            hl7_parsed.update({label: value})
            
        # update return message
        message['hl7_parsed'] = hl7_parsed

    except Exception as e:
        logging.error(f"HL7_PARSE_ERROR - Error Parsing HL7 Message - {type(e).__name__} - {str(e)}")
        message['log_severity_cd'] = 'ERROR'
        message['log_message_cd'] = 'HL7_PARSE_ERROR'
        message['more_info'] = {'summary': type(e).__name__, 'detail': str(e)}

    return message
