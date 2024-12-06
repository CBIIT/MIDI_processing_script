

####SCRIPT

from googleapiclient import discovery 
from oauth2client.client import GoogleCredentials 
import pydicom as pydcm 
import pandas as pd 
import sys 
import numpy as np 
import random 
import time 
import datetime 
import datefinder 
from datetime import datetime, timedelta, date 
from google.cloud import storage 
from io import BytesIO 
import re 
from pydicom.datadict import dictionary_VR 
from pydicom import dcmread,Dataset 
from pydicom.filewriter import dcmwrite 
from pydicom import config 
import requests  
import ndjson 
import jmespath 
 

 

config.settings.reading_validation_mode = config.IGNORE 
client = storage.Client() 
 
credentials = GoogleCredentials.get_application_default() 
api_version = "v1beta" 
service_name = "healthcare" 
service = discovery.build('healthcare', 'v1beta1', credentials=credentials) 
 
 
def create_dataset(project_id,location, dataset_id): 
    parent = 'projects/{}/locations/{}'.format(project_id, location) 
    request = service.projects().locations().datasets().create(parent=parent, body={}, datasetId=dataset_id) 
    response = request.execute() 
    print (f"{dataset_id} created") 
    return response 
 
 
def create_dicom_store(project_id, location, dataset_id, dicom_store_id): 
    dicom_store_parent = "projects/{}/locations/{}/datasets/{}".format(project_id, location, dataset_id) 
    request = service.projects().locations().datasets().dicomStores().create(parent=dicom_store_parent, body={}, dicomStoreId=dicom_store_id) 
    response = request.execute() 
    print(f"{dicom_store_id} created in {dataset_id}") 
    return response 
 
 
def create_fhir(project_id, location, dataset_id): 
    parent = 'projects/{}/locations/{}'.format(project_id, location) 
    request = service.projects().locations().datasets().create(parent=parent, body={}, datasetId=dataset_id) 
    response = request.execute() 
    time.sleep(2) 
    fhir_store_parent = "projects/{}/locations/{}/datasets/{}".format(project_id, location, dataset_id) 
    body = {"version": 'R4', "enableUpdateCreate": True} 
    request = service.projects().locations().datasets().fhirStores().create(parent=fhir_store_parent, body=body, fhirStoreId='fhirdata') 
    response = request.execute() 
    print(f"fhirdata created in {dataset_id}") 
    return response 
 
def import_data(project_id, location, dataset_id, dicom_store_id, content_uri): 
    name = "projects/{}/locations/{}/datasets/{}/dicomStores/{}".format(project_id, location, dataset_id, dicom_store_id) 
    import_dicom_data_request_body = {"gcsSource": {"uri": f"gs://{content_uri}"}} 
    request = service.projects().locations().datasets().dicomStores().import_(name=name, body=import_dicom_data_request_body) 
    response = request.execute() 
 
 
def check_status(project_id, location, dataset_id, step): 
    name = "projects/{}/locations/{}/datasets/{}".format(project_id, location, dataset_id) 
    request = service.projects().locations().datasets().operations().list(name=name) 
    while request.execute().get('operations', [])[0]['metadata']['apiMethodName'].split('.')[-1]!=step: 
        time.sleep(1) 
    while 'done' not in request.execute().get('operations', [])[0]: 
        time.sleep(10) 
    print (f'{step} complete') 
 
def deidentify_dataset(project_id, location, source_dataset_id, source_store_id, destination_dataset_id, destination_store_id, fhir_dataset): 
    source_store = "projects/{}/locations/{}/datasets/{}/dicomStores/{}".format(project_id, location, source_dataset_id, source_store_id) 
    destination_store = "projects/{}/locations/{}/datasets/{}/dicomStores/{}".format(project_id, location, destination_dataset_id, destination_store_id) 
    fhir_store = "projects/{}/locations/{}/datasets/{}/fhirStores/{}".format(project_id, location, fhir_dataset, 'fhirdata') 
 
    deidentify_dataset_request_body = { 
        "destinationStore": destination_store, 
        "config":{ 
            'dicomTagConfig':{ 
                "actions": [ 
                {"queries": [ 
                'AT', 'CS', 'DS', 'FL', 'FD', 'IS', 'OD', 'OF', 'SL', 'SS', 'US', 'TM', 
                'DA','DT', 
                '00091008','70051008','7005100b','7005100e','7005100f','70051012','70051017','70051018','70051019','7005101a','7005101b',
               '7005101e','70051020','70051030','001910a3','00271033','00431035','00431036','00431037','00100040','00102203','00101020',
               '00101030','001021c0','001021a0','00281054','20100010','00080070','00101010','00080016','00081150',
               '00020002','00020010',
               '00020003',
               '00100020',
               '00080100',
               '00210023',
               '00310010', '00310011', '00310012', '00390010', 
               '0099000a', '01370050',
               '09050010', '09070010', '10990010', '20010090',  '200b0010', '200b0070',
               '200b0072', '31090010', '31090011', '37110010', '7fdf0010',
               '00200011','00190010','00190011','00190012','00190013','00190014','00190015','00290010','00290011','00290012','00290013','00290014',
               '00290015','00130010','00130011','00130012','00130013','00130014','00130015','44530010','44530011','44530012','44530013',
               '44530014','44530015','00E10010','00E10011','00E10012','00E10013','00E10014','00E10015','01E10010','01E10011','01E10012',
               '01E10013','01E10014','01E10015','01f10010','01f10011','00f10012','00f10013','00f10014','00f10015','11290010','11290011',
               '11290012','11290013','11290014','11290015','00090010','00090011','00090012','00090013','00090014','00090015','00210010',
               '00210011','00210012','00210013','00210014','00210015','00250010','00250011','00250012','00250013','00250014','00250015',
               '50F10010','50F10011','50F10012','50F10013','50F10014','50F10015','00530010','00530011','00530012','00530013','00530014',
               '00530015','7fd10010','7fd10011','7fd10012','7fd10013','7fd10014','7fd10015','09030010','09030011','09030012','09030013',
               '09030014','09030015','004b0010','004b0011','004b0012','004b0013','004b0014','004b0015','00510010','00510011','00510012',
               '00510013','00510014','00510015','00110010','00110011','00110012','00110013','00110014','00110015','00150010','00150011',
               '00150012','00150013','00150014','00150015','00330010','00330011','00330012','00330013','00330014','00330015','00350010',
               '00350011','00350012','00350013','00350014','00350015','00550010','00550011','00550012','00550013','00550014','00550015',
               '00450010','00450011','00450012','00450013','00450014','00450015','00270010','00270011','00270012','00270013','00270014',
               '00270015','70010010','70010011','70010012','70010013','70010014','70010015','00430010','00430011','00430012','00430013',
               '00430014','00430015','00230010','00230011','00230012','00230013','00230014','00230015','7E010010','7e010011','7e010012',
               '7e010013','7e010014','7e010015','3F010010','3f010011','3f010012','3f010013','3f010014','3f010015','20010010','20010011',
               '20010012','20010013','20010014','20010015','20050010','20050011','20050012','20050013','20050014','20050015','70530010',
               '70530011','70530012','70530013','70530014','70530015','60010010','60010011','60010012','60010013','60010014','60010015',
               '00410010','00410011','00410012','00410013','00410014','00410015','00570010','00570011','00570012','00570013','00570014',
               '00570015','00610010','00610011','00610012','00610013','00610014','00610015','7FE30010','7fe30011','7fe30012','7fe30013',
               '7fe30014','7fe30015','00710010','00710011','00710012','00710013','00710014','00710015','00170010','00170011','00170012',
               '00170013','00170014','00170015','00990010','00990011','00990012','00990013','00990014','00990015','70050010','70050011',
               '70050012','70050013','70050014','70050015','01170010','01170011','01170012','01170013','01170014','01170015','01190010',
               '01190011','01190012','01190013','01190014','01190015','01f70010','01f70011','01f70012','01f70013','01f70014','01f70015',
               '07a10010','07a10011','07a10012','07a10013','07a10014','07a10015','07a30010','07a30011','07a30012','07a30013','07a30014',
               '07a30015','07a50010','07a50011','07a50012','07a50013','07a50014','07a50015','32870010','32870011','32870012','32870013',
               '32870014','32870015','50010010','50010011','50010012','50010013','50010014','50010015'
                ], 
                "keepTag": {} 
                },
               {"queries": [
               'PN','00120051','00080094','00101040','00102154','00080081',
               '00080092','00400001','00404036','00100021','00402010','001021f0','00081049','00321033','00380060','00321021',
               '00081040','00380061','00402009','00401004','00081062','00102299','00380010','00101050','04000404','00101000',
               '00400243','00400253','00102297','00320012','00403001','00401011','00101090','00101002','00081052','00380004',
               '40080042','00880910','00380300','4008010c','00203401','00101001','00081050','00080096','00880912','00181008',
               '40080202','00081060','00101005','00401102','00102152','00404034','00181005','4008011a','40080102',
               '00401010','00380400','00401103','00401001','0038001e','00880904','00400241','40080119','4008010a','00100050',
               '40080114','00401005','0040000b','00181004','00404037','00380011','00101060','00081048','00321032','00400006',
               '00880906','00404035','0040a07c','40080111','0040a07a','00101100','00321020','00401104','0040a353','fffcfffc',
               '00380014','00100032','00081120','40080118','40000010','00181007','00380064','00100101','300a0216','0040a078',
               'fffafffa','00101080','00100102','04000403','00101081','00880200','04000550','00102155','0008009d',
               '00203406','00404030','00400011','0040a354','00700086','04000600','00402011','00400242','00404027','40004000',
               '00102150','00081041','00402008','00404028','00400010','20300020','0040a307','04000561','04000402','00404025',
               '0040a352','0040a358','00284000','00131013','00131012','00431080','000910c9','00091099','00091002',
               '000910d2','00091096','000910c8','00091030','00431063','00091082','00091058','00080201','00400009',
               '00080080', '00081010',
               '00080054',
               '00080050','0008009c','00700084','00402017','00100030','00100010',
               '00402016','00080090','0040a088','0018700a','00081072','300e0008','30080105','00081110',
               '300a00b2','00081070','00081111','00080082','00120021','00120030','00120031','00120050','00200010',
               '00181010','00181011','00041500'
                ], 
                'removeTag': {} 
                }, 
                {"queries": [ 
                '00401101','0040a123', 
                '0040a075','0040a073','0040a027',
                '00120020','00120040','00120042'
                ], 
                'resetTag': {} 
                }, 
                {"queries": [ 
                    'AE', 'LO', 'LT', 'SH', 'ST', 'UC', 'UT', 'AS', 
                    '0019109c','00131010','00080022','0008002a','00380020','00080023', 
                    '00080025','00181200','0018700c','00181012','00080012','001021d0','00080024','00400250','00400244','00181078','00181079','00321000', 
                    '00080021','00400004','00400002','30060008','00321040','00321050','00080020','00184000','00181400','00189424','001021b0','00081080', 
                    '00102110','00400280','00180010','0018a003','00082111','00380040','00209158','00084000','00204000','00402400','40080300','40080115', 
                    '4008010b','00081090','00102000','00104000','00380500','00400254','00400012','00402001','00321030','00321070','00401400','00321060', 
                    '40084000','0008103e','00380062','00181020','00380050','00400007','00324000','00081030','00384000','00203404','00181030','00102180' 
                    ], 
                "cleanTextTag": {} 
                }, 
                {"queries": [ 
                    'PixelData'], 
                "cleanImageTag": {"textRedactionMode": 'REDACT_SENSITIVE_TEXT'} 
                }, 
                {"queries": ['SQ'], 
                "recurseTag": {} 
                }, 
                {"queries": ['UI'], 
                "regenUidTag": {} 
                }], 
                "profileType": 'DEIDENTIFY_TAG_CONTENTS'}, 
            "text":{ 
                "additionalTransformations": [ 
                    { 
                        "infoTypes": [], 
                        "replaceWithInfoTypeConfig": {} 
                    } 
                ], 
                "additionalTransformations": [ 
                    { 
                    "infoTypes": [ 
                    'STREET_ADDRESS'], 
                    "redactConfig": {} 
                    },                     
                    { 
                    "infoTypes": [], 
                    "redactConfig": {} 
                    } 
                ], 
            }, 
            "operationMetadata": { 
            "fhirOutput": { 
                "fhirStore": fhir_store 
              } 
          } 
        } 
    } 
 
    request = service.projects().locations().datasets().dicomStores().deidentify(sourceStore=source_store, body=deidentify_dataset_request_body) 
    response = request.execute() 
 

def export_data(project_id, location, dataset_id, dicom_store_id, uri_prefix): 
    name = "projects/{}/locations/{}/datasets/{}/dicomStores/{}".format(project_id, location, dataset_id, dicom_store_id) 
    export_dicom_data_request_body = {"gcsDestination": {"uriPrefix": f"gs://{uri_prefix}"}} 
    request = service.projects().locations().datasets().dicomStores().export(name=name, body=export_dicom_data_request_body) 
    response = request.execute() 
 
 
def export_fhir(project_id, location, dataset_id, uri_prefix): 
    name = "projects/{}/locations/{}/datasets/{}/fhirStores/{}".format(project_id, location, dataset_id, 'fhirdata') 
    export_fhir_data_request_body = {"gcsDestination": {"uriPrefix": f"gs://{uri_prefix}"}} 
    request = service.projects().locations().datasets().fhirStores().export(name=name, body=export_fhir_data_request_body) 
    response = request.execute() 
 

def uid_mapping(bucketname, fhirprefix, mapping_folder): 
    bucket = client.get_bucket(bucketname) 
    blobs = bucket.list_blobs(prefix=fhirprefix) 
    filelist = [item.name for item in blobs] #extracting file names from blobs 
    blob = bucket.get_blob(filelist[0]) 
    #fhir = blob.download_to_filename("fhirtest") 
    json_data_string = blob.download_as_string() 
    fhir = ndjson.loads(json_data_string) 

    tag = [] 
    og_text = [] 
    new_text = [] 
 
    for filei in range(0, len(fhir)): 
        try: 
            fhir2 = fhir[filei]['entity'][0]['extension'][0]['extension'] 
        except: 
            continue 
        for i in range(0, len(fhir2)): 
            if fhir2[i]['extension'][2]['valueString'] == 'Regenerated UID': 
                #print('Tag: '+fhir2[i]['valueString']) 
                tag.append(fhir2[i]['valueString']) 
                #print('Original Text :'+fhir2[i]['extension'][0]['valueString']) 
                og_text.append(fhir2[i]['extension'][0]['valueString']) 
                #print('New Text :'+fhir2[i]['extension'][1]['valueString']) 
                new_text.append(fhir2[i]['extension'][1]['valueString']) 
 
    uid_mapping = pd.DataFrame({'tag': tag, 'og_uid': og_text, 'new_uid': new_text}) 
 
    uid_mapping.to_csv('uid_mapping.csv') 
 
 
    blob = bucket.blob(mapping_folder) 
    blob.upload_from_filename('uid_mapping.csv') 
    return 
 
