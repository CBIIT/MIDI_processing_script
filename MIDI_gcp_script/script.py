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
config.settings.reading_validation_mode = config.IGNORE
client = storage.Client()

credentials = GoogleCredentials.get_application_default()
api_version = "v1beta"
service_name = "healthcare"
service = discovery.build('healthcare', 'v1beta1', credentials=credentials)


keeplist = ['(0009, 1008)','(7005, 1008)','(7005, 100b)','(7005, 100e)','(7005, 100f)','(7005, 1012)','(7005, 1017)','(7005, 1018)','(7005, 1019)','(7005, 101a)','(7005, 101b)',
               '(7005, 101e)','(7005, 1020)','(7005, 1030)','(0019, 0014)','(0019, 10a3)','(0027, 1033)','(0043, 1035)','(0043, 1036)','(0043, 1037)','(0010, 0040)','(0010, 2203)','(0010, 1020)',
               '(0010, 1030)','(0010, 21c0)','(0010, 21a0)','(0008, 0018)','(0020, 000d)','(0020, 000e)','(0028, 1054)','(2010, 0010)','(0008, 0070)','(0010, 1010)','(0008, 0016)','(0008, 1150)',
               '(0002, 0002)','(0002, 0010)',
               '(0002, 0003)']

file = 0
#vrs to look at
vrs = ['LO', 'SH', 'LT', 'ST', 'UT','IS']
str1 = ''
x = []
x2 = []

#these aren't really necessary for final code. just used for testing
patient = []
study = []
SOP = []
series = []
element = []
tag = []
value = []
vr = []
newvalue = []
DICOM = []

#regex rules
loc = re.compile('at [A-Z]{2,4}')
dr = re.compile('by [A-Z]{2}')

#blacklist
blacklist = [' MGH', ' ALH']

#whitelist of common radiology acronyms. Will probably need to be expanded
whitelist = ['CT', 'MRI', 'MR', 'CTA', 'PA', 'AP','CXR','IV','LLQ','LUQ','PET','RLQ','RUQ','US','NM','RT']

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

def deidentify_dataset(project_id, location, source_dataset_id, destination_dataset_id):
    source_dataset = "projects/{}/locations/{}/datasets/{}".format(project_id, location, source_dataset_id)
    destination_dataset = "projects/{}/locations/{}/datasets/{}".format(project_id, location, destination_dataset_id)


    deidentify_dataset_request_body = {
        "destinationDataset": destination_dataset,
        "config":{
            'dicomTagConfig':{
                "actions": [
                {"queries": [
                'AT', 'CS', 'DS', 'FL', 'FD', 'IS', 'OD', 'OF', 'SL', 'SS', 'US', 'TM',
                'DA','DT',
                '00091008','70051008','7005100b','7005100e','7005100f','70051012','70051017','70051018','70051019','7005101a','7005101b',
                '7005101e','70051020','70051030','001910a3','00271033','00431035','00431036','00431037','00100040','00102203','00101020',
                '00101030','001021c0','001021a0','00080018','0020000d','0020000e','00281054','20100010','00080070','00101010','00080016','00081150',
                '00020002','00020010','00020003', '00100020', '00080100',
                '00090010', '00090011', '00090012', '00090013', '00110010', '00110011', '00130011', '00170010','00190010',
                '00190012', '00190014', '00190015', '00210010', '00210011', '00210013', '00210023', '00230010', '00250010',
                '00270010', '00290010', '00290011', '00310010', '00310011', '00310012', '00330010', '00390010', '00430010',
                '00450010', '004b0010', '00510010', '0099000a', '00e10010', '01370050', '01e10010', '01f10010', '01f70010',
                '07a10010', '07a30010', '07a50010', '09030010', '09050010', '09070010', '10990010', '11290010', '20010010',
                '20010011', '20010090', '20050010', '20050011', '20050012', '20050013', '20050014', '200b0010', '200b0070',
                '200b0072', '31090010', '31090011', '37110010', '44530010', '70530010', '7fd10010', '7fdf0010'
                ],
                "keepTag": {}
                },
                {"queries": [
                'PN','00120051','00120020','00120040','00120042','00181002','04000100','00880140','00080094','00101040','00102154','00080081',
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
                '0040a352','0040a358','00284000','00131013','00131012','00200011','00431080','000910c9','00091099','00091002',
                '000910d2','00091096','000910c8','00091030','00431063','00091082','00091058','00080201','00400009',
                '00080080', '00081010',
                '00080054'
                ],
                'removeTag': {}
                },
                {"queries": [
                '00080050','0008009c','00700084','00402017','00100030','00100010','00401101','0040a123',
                '00402016','00080090','0040a088','0040a075','0040a073','0040a027','0018700a','00081072','300e0008','30080105','00081110',
                '300a00b2','00081070','00081111','00080082','00120021','00120030','00120031','00120050','00200010'
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
                    },                     {
                    "infoTypes": [],
                    "redactConfig": {}
                    }
                ],
            }
        }
    }

    request = service.projects().locations().datasets().deidentify(sourceDataset=source_dataset, body=deidentify_dataset_request_body)
    response = request.execute()


def export_data(project_id, location, dataset_id, dicom_store_id, uri_prefix):
    name = "projects/{}/locations/{}/datasets/{}/dicomStores/{}".format(project_id, location, dataset_id, dicom_store_id)
    export_dicom_data_request_body = {"gcsDestination": {"uriPrefix": f"gs://{uri_prefix}"}}
    request = service.projects().locations().datasets().dicomStores().export(name=name, body=export_dicom_data_request_body)
    response = request.execute()


def blob_to_image(bucket, blob_name):
    blob = bucket.get_blob(blob_name)
    blobstring = blob.download_as_string()
    dicomfile = BytesIO(blobstring)
    dicomread = dcmread(dicomfile)
    return dicomread

def img_diff(raw, deid):
    diff = raw.pixel_array-deid.pixel_array
    result = np.count_nonzero(diff)
    return(result)    

def find(list,thing): 
    "finds indices of something in a list"
    return [i for i, j in enumerate(list) if j == thing]

def blob_to_df(filelist, bucket):
    SOP = []
    patient = []
    study = []
    series = []
    DICOM = []
    for i in range(len(filelist)):
        dicom = blob_to_image(bucket, filelist[i])
        DICOM.append(dicom)
        patient.append(dicom.PatientID)
        study.append(dicom.StudyInstanceUID)
        series.append(dicom.SeriesInstanceUID)
        SOP.append(dicom.SOPInstanceUID)
    d = {'dicom': DICOM, 'patientID': patient, 'studyID': study, 'seriesID': series, 
         'SOPID': SOP}
    df = pd.DataFrame(d)
    print ('df created')
    return(df)

def finddates(text):
    today = datetime.today()
    thisyear = today.year
    dt = []
    
    
    yyyymmdd = re.compile('(19[0-9][0-9]|20[0-2][0-9])(0[1-9]|1[0-2]|[1-9])(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])')
    match = yyyymmdd.match(text)
    if match is not None:
        m = match.group(2)
        if m[0] == 0:
            m = m[1:]
        d = match.group(3)
        if d[0] == 0:
            d = d[1:]
        y = match.group(1)
        end = match.end()
        dt.append([y,m,d,end,1])
   
    #only checks years up to 2022. This would need to be changed
    #also need to change the order in which date formats are checked. yyyymmdd should be checked first
    mmddyyyy = re.compile('(0[1-9]|1[0-2]|[1-9])(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])(19[0-9][0-9]|20[0-2][0-9])')
    match = mmddyyyy.match(text)
    if match is not None:
        m = match.group(1)
        if m[0] == 0:
            m = m[1:]
        d = match.group(2)
        if d[0] == 0:
            d = d[1:]
        y = match.group(3)
        end = match.end()
        dt.append([y,m,d,end,10])
        
    ddmmyyyy = re.compile('(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])(0[1-9]|1[0-2]|[1-9])(19[0-9][0-9]|20[0-2][0-9])')
    match = ddmmyyyy.match(text)
    if match is not None:
        m = match.group(2)
        if m[0] == 0:
            m = m[1:]
        d = match.group(1)
        if d[0] == 0:
            d = d[1:]
        y = match.group(3)
        end = match.end()
        dt.append([y,m,d,end,11])
    

    
    
    yyyyddmm = re.compile('(19[0-9][0-9]|20[0-2][0-9])(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])(0[1-9]|1[0-2]|[1-9])')
    match = yyyyddmm.match(text)
    if match is not None:
        m = match.group(3)
        if m[0] == 0:
            m = m[1:]
        d = match.group(2)
        if d[0] == 0:
            d = d[1:]
        y = match.group(1)
        end = match.end()
        dt.append([y,m,d,end,12])

    
    
    yyyy_mm_dd = re.compile('(19[0-9][0-9]|20[0-2][0-9])(\.|/| |-)(0[1-9]|1[0-2]|[1-9])(\.|/| |-)(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])')
    match = yyyy_mm_dd.match(text)
    if match is not None:
        m = match.group(3)
        if m[0] == 0:
            m = m[1:]
        d = match.group(5)
        if d[0] == 0:
            d = d[1:]
        y = match.group(1)
        end = match.end()
        dt.append([y,m,d,end,2])
    
    mm_dd_yyyy = re.compile('(0[1-9]|1[0-2]|[1-9])(\.|/| |-)(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])(\.|/| |-)(19[0-9][0-9]|20[0-2][0-9])')
    match = mm_dd_yyyy.match(text)
    if match is not None:
        m = match.group(1)
        if m[0] == 0:
            m = m[1:]
        d = match.group(3)
        if d[0] == 0:
            d = d[1:]
        y = match.group(5)
        end = match.end()
        dt.append([y,m,d,end,3])
    
    dd_mm_yyyy = re.compile('(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])(\.|/| |-)(0[1-9]|1[0-2]|[1-9])(\.|/| |-)(19[0-9][0-9]|20[0-2][0-9])')
    match = dd_mm_yyyy.match(text)
    if match is not None:
        m = match.group(3)
        if m[0] == 0:
            m = m[1:]
        d = match.group(1)
        if d[0] == 0:
            d = d[1:]
        y = match.group(5)
        end = match.end()
        dt.append([y,m,d,end,4])
    
    
    yyyy_dd_mm = re.compile('(19[0-9][0-9]|20[0-2][0-9])(\.|/| |-)(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])(\.|/| |-)(0[1-9]|1[0-2]|[1-9])')
    match = yyyy_dd_mm.match(text)
    if match is not None:
        m = match.group(5)
        if m[0] == 0:
            m = m[1:]
        d = match.group(3)
        if d[0] == 0:
            d = d[1:]
        y = match.group(1)
        end = match.end()
        dt.append([y,m,d,end,5])
    
    
    #for dates with delimiters (. / ' ' or -), yy is a valid year format. So 10-23-98 would count as a valid date along with 10-23-1998
    #for yy year formats, will default to 2000s unless that would make the year greater than the current year, in which case 1900 will be used
    #so if the yy is 15, the year will be changed to 2015. if the yy is 45 the year will be changed to 1945
    y2_mm_dd = re.compile('([0-9][0-9])(\.|/| |-)(0[1-9]|1[0-2]|[1-9])(\.|/| |-)(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])')
    match = y2_mm_dd.match(text)
    if match is not None:
        m = match.group(3)
        if m[0] == 0:
            m = m[1:]
        d = match.group(5)
        if d[0] == 0:
            d = d[1:]
        y = match.group(1)
        if int('20'+y)<=thisyear:
            y = '20'+y
        else:
            y = '19'+y
        end = match.end()
        dt.append([y,m,d,end,6])
    
    dd_mm_y2 = re.compile('(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])(\.|/| |-)(0[1-9]|1[0-2]|[1-9])(\.|/| |-)([0-9][0-9])')
    match = dd_mm_y2.match(text)
    if match is not None:
        m = match.group(3)
        if m[0] == 0:
            m = m[1:]
        d = match.group(1)
        if d[0] == 0:
            d = d[1:]
        y = match.group(5)
        if int('20'+y)<=thisyear:
            y = '20'+y
        else:
            y = '19'+y
        end = match.end()
        dt.append([y,m,d,end,7])
    
    y2_dd_mm= re.compile('([0-9][0-9])(\.|/| |-)(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])(\.|/| |-)(0[1-9]|1[0-2]|[1-9])')
    match = y2_dd_mm.match(text)
    if match is not None:
        m = match.group(5)
        if m[0] == 0:
            m = m[1:]
        d = match.group(3)
        if d[0] == 0:
            d = d[1:]
        y = match.group(1)
        if int('20'+y)<=thisyear:
            y = '20'+y
        else:
            y = '19'+y
        end = match.end()
        dt.append([y,m,d,end,8])
    
    mm_dd_y2 = re.compile('(0[1-9]|1[0-2]|[1-9])(\.|/| |-)(0[1-9]|1[0-9]|2[0-9]|3[0-1]|[1-9])(\.|/| |-)([0-9][0-9])')
    match = mm_dd_y2.match(text)
    if match is not None:
        m = match.group(1)
        if m[0] == 0:
            m = m[1:]
        d = match.group(3)
        if d[0] == 0:
            d = d[1:]
        y = match.group(5)
        if int('20'+y)<=thisyear:
            y = '20'+y
        else:
            y = '19'+y
        end = match.end()
        dt.append([y,m,d,end,9])
    
    
    if len(dt)==0:
        return(None)
    elif len(dt)==1:
        return dt[0]
    else:
        sorted(dt, key = lambda x: (-x[3], x[4]))
        return dt[0]

def post_process(df):
    file = 0
    #vrs to look at
    vrs = ['LO', 'SH', 'LT', 'ST', 'UT','IS']
    #minimum valid date is 1900-01-02
    mindt = datetime(1900,1,2)
    maxdt = datetime.now()
    orgtextlist = []
    newtextlist = []
    orglist = []
    newlist = []
    taglist = []
    ilist = []
    loc = re.compile('at ([A-Z]{2,4})')
    dr = re.compile('by ([A-Z]{2})')
    
    #loops through each dicom file
    for f in range(len(df)):
        file = df.iloc[f][0]
        
        #creates random int between 1 and 100 using the series id as a seed to use as dateshift
        random.seed(df.iloc[f][3])
        randdelta = random.randint(1,100)
        
        
        #creates random number between 100000-999999 to use as patient id using the current patient ID as a seed
        #might need to change this method to make it more secure
        random.seed(file.PatientID)
        newpid = random.randint(100000,999999)
        file.PatientID = str(newpid)
        
        #loops through each element in the dicom file
        for elem in file.iterall():
            
            #shifts dates in DA VRs
            if elem.name == 'Text Value':
                    elem.value = '1'
            if elem.VR == 'DA':
                str1 = str(elem.value)
                orglist.append(str1)
                #appends whitespace to dates bc otherwise datefinder wont find the date
                str1 = ' '+str1+' '
                #looks for dates in DA elem using datefinder package (different from the one I created)
                #this could potentially be changed in future versions to just using regex with format yyyymmdd
                x = datefinder.find_dates(str1, source=True, index=False, strict=False, base_date=None)
                #loops through found dates (should just be 1 date)
                for i in x:
                    dt = i[0]
                    org = i[1]
                    #shifts the date back 1-100 days using the previously created random number
                    dt = dt - timedelta(days = randdelta)
                    #takes the year month and day of the new date
                    y = str(dt.year)
                    m = str(dt.month)
                    d = str(dt.day)
                    #changes month to mm format if in m
                    if len(m) == 1:
                        m = '0'+m
                    #changes day to dd format if in d
                    if len(d) == 1:
                        d = '0'+d
                    newstr = y+m+d
                    #replaces string with new date in yyyymmdd format
                    str1 = str1.replace(org,newstr)
                    newtextlist.append(newstr)
                    orgtextlist.append(org)
                    ilist.append(f)
                    taglist.append(elem.name)
                #replaces element value with new string (without the extra whitespaces)
                elem.value = str1[1:len(str1)-1]
                newlist.append(str1[1:len(str1)-1])
                
            #does the same exact thing as above, but for DT
            if elem.VR == 'DT':
                str1 = str(elem.value)
                orglist.append(str1)
                if '.' in str1:
                    str1 = str1[:str1.index('.')]
                str1 = ' '+str1+' '
                x = datefinder.find_dates(str1, source=True, index=False, strict=False, base_date=None)
                for i in x:
                    dt = i[0]
                    org = i[1]
                    dt = dt - timedelta(days = randdelta)
                    y = str(dt.year)
                    m = str(dt.month)
                    d = str(dt.day)
                    h = str(dt.hour)
                    mi = str(dt.minute)
                    sec = str(dt.second)
                    if len(m) == 1:
                        m = '0'+m
                    if len(d) == 1:
                        d = '0'+d
                    #creates new shifted date in format yyyymmddhourminsec
                    newstr = y+m+d+h+mi+sec
                    str1 = str1.replace(org,newstr)
                    newtextlist.append(newstr)
                    orgtextlist.append(org)
                    ilist.append(f)
                    taglist.append(elem.name)
                elem.value = str1[1:len(str1)-1]
                newlist.append(str1[1:len(str1)-1])
                
            #ignores elements not of VR 'LO', 'SH', 'LT', 'ST', 'UT','IS'
            #ignores Patient ID, Code Value, Private Creator, Actual Frame Duration, and Primary Counts Accumulates elements
            elif elem.VR in vrs and elem.name != 'Patient ID' and elem.name != 'Code Value' and elem.name != 'Private Creator' and elem.name != 'Actual Frame Duration' and elem.name != 'Primary (Prompts) Counts Accumulated':
                #ignores tags in automatic keep list
                if elem.tag not in keeplist:
                    str1 = str(elem.value)
                    
                    #searches for match with location regex ('at 'ABC)
                    matchloc = loc.search(str1)
                    if matchloc is not None:
                        #if there's a match, checks the acronym isn't in the whitelist
                        if matchloc.group(1) not in whitelist:
                            #if it's not in the whitelist, replaces the acronym with '1'
                            strloc = loc.sub('1',str1)
                            #the append statements are just for creating a df to double check results, not a part of the de-id process
                            taglist.append(elem.name)
                            ilist.append(f)
                            orglist.append(str1)
                            orgtextlist.append(matchloc.group(0))
                            str1 = strloc
                            newlist.append(str1)
                            newtextlist.append('1')
                            #updates elem value with new string
                            elem.value = str1
                    #does the same as above, but for the doctor initial regex ('by 'AB)
                    matchdr = dr.search(str1)
                    if matchdr is not None:
                        if matchdr.group(1) not in whitelist:
                            strdr = dr.sub('1',str1)
                            taglist.append(elem.name)
                            ilist.append(f)
                            orglist.append(str1)
                            orgtextlist.append(matchdr.group(0))
                            str1 = strdr
                            newlist.append(str1)
                            newtextlist.append('1')
                            elem.value = str1
                    
                    #date/patient id finder
                    #creates list of strings made up of digits and certain delimiters (- / . and ' ') where the first character is a digit and the length is 6-15
                    x = re.findall('[\d][\d -/\.]{6,15}', str1)
                    if len(x)!=0:
                        #loops through the strings list
                        for n in x:
                            #checks that the first digit is numeric. this is a reduntant part of the code that wasn't deleted on accident
                            if n[0].isnumeric():
                                #checks if the string is/contains a date
                                date = finddates(n)
                                if date is not None:
                                    #takes the end index of the date within the string
                                    end = int(date[3])
                                    #takes the year month and day of each date and puts into a datetime format
                                    orgdate = datetime(int(date[0]),int(date[1]),int(date[2]))
                                    #shifts the date back 1-100 days using random number
                                    newdate = orgdate-timedelta(days = randdelta)
                                    #identifies original date string using end index
                                    orgstring = n[0:end]
                                    #takes the string of the newdate, cutting off the hour/min/sec values
                                    #new str is in format yyyy-mm-dd. might potentially change this so that '-'s are removed
                                    newstr = str(newdate)[:10]
                                    orglist.append(str1)
                                    orgtextlist.append(orgstring)
                                    #replaces the original date string with the new date string within the element string
                                    str1 = str1.replace(orgstring,newstr)
                                    #again all of the appends are not part of the de-id process
                                    taglist.append(elem.name)
                                    ilist.append(f)
                                    
                                    #if VR is IS, replace element value with '1' and update the element value
                                    if elem.VR == 'IS':
                                        elem.value = '1'
                                        newtextlist.append('1')
                                        newlist.append('1')
                                    else:
                                        #these are yet again just for keeping track of what the post processing script is doing
                                        newtextlist.append(newstr)
                                        newlist.append(str1)
                                        
                                #if the string of digits/delimiters doesn't contain a date and isn't IS
                                elif elem.VR != 'IS':
                                    #finds all numeric only strings of length 6-15 within the previously identified string of digits and delimiters
                                    longints = re.findall('[0-9]{6,15}', str1)
                                    if longints is not None:
                                        #replaces each of these long strings of integers with a '1'
                                        for orgint in longints:
                                            index = str1.find(orgint)
                                            if index > 1 and str1[index-2].isnumeric and str1[index-1] == '.':
                                                continue
                                            else:
                                                taglist.append(elem.name)
                                                ilist.append(f)
                                                orglist.append(str1)
                                                orgtextlist.append(orgint)
                                                str1 = str1.replace(orgint,'1')
                                                taglist.append(elem.name)
                                                newtextlist.append('1')
                                                newlist.append(str1)
                        #after looping through the list of numeric strings, updates element with all changes made to the element string
                        #only if VR isn't IS, as all IS VR tags with dates in them have already been changed to 1
                        if elem.VR != 'IS':
                            elem.value = str1


