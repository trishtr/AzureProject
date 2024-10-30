import json
from hl7parser.hl7 import HL7Message
from hl7parser.hl7_data_types import HL7_VersionIdentifier
from hl7apy.parser import parse_message

INVALID_VERSION_MAP = {'2.1': '2.5.1', '2.7.1': '2.8', '2.9': '2.8', '2.9.1': '2.8'}

# from code/code-values; input parameter/adf; 
# -- REMOVE FOR AZURE FUNCTION
client_config = {
  'msh.msh_3.msh_3_1': 'sending_application',
  'msh.msh_4.msh_4_1': 'sending_facility',
  'msh.msh_5.msh_5_1': 'receiving_application',
  'msh.msh_6.msh_6_1': 'receiving_facility',
  'msh.msh_7': 'message_dttm',
  'msh.msh_10': 'message_control_id',
  'msh.msh_11': 'processing_type_cd',
  'msh.msh_12': 'version_id',
  'msh.msh_9.msh_9_1': 'message_type_cd',
  'msh.msh_9.msh_9_2': 'trigger_event_type_cd',
  'evn.evn_2': 'recorded_dttm',
  'pv1.pv1_19.pv1_19_1': 'visit_number',        # cayuga: pid.pid_18.pid_18_1
  'pv1.pv1_2': 'patient_class_cd',
  'pv1.pv1_3.pv1_3_1': 'point_of_care_cd',
  'pv1.pv1_3.pv1_3_2': 'room',
  'pv1.pv1_3.pv1_3_3': 'bed',
  'pv1.pv1_3.pv1_3_4': 'assigned_facility',
  'pv1.pv1_4': 'admission_type_cd',
  'pv1.pv1_10': 'hospital_service_cd',
  'pv1.pv1_14': 'admit_source_cd',
  'pv1.pv1_18': 'patient_type_cd',
  'pv1.pv1_36': 'discharge_disposition_cd',
  'pv1.pv1_44': 'admit_dttm',
  'pv1.pv1_45': 'discharge_dttm'
} 



hl7_message = ""


pm1 = HL7Message(hl7_message)
print(pm1)
print('-------------------------')
current_version_id = str(pm1.msh.version_id[0])


if current_version_id in INVALID_VERSION_MAP:
  pm1.msh.version_id.set_attributes(
      pm1.msh.version_id.field_map, [INVALID_VERSION_MAP[current_version_id]]
    )

  hl7_message = '\r'.join(str(pm1).splitlines())
  print(pm1)
 
# step 2 - parse message
# -- uses hl7apy.parser.parse_message



pm2 = parse_message(hl7_message)
client_fields = {}

for hl7key, label in client_config.items():
  hl7key_parts = hl7key.split('.')  
  hl7_object = pm2 

  for item in hl7key_parts:
    hl7_object = getattr(hl7_object, item, None) 
    if hl7_object is None:
      break
    else:
      value = hl7_object.value.strip()

  client_fields.update({label: value})

print(json.dumps(client_fields))