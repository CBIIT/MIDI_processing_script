import base64
import functions_framework
import time
from script import *
from pydicom import config
config.settings.reading_validation_mode = config.IGNORE
client = storage.Client()

project = 'nih-nci-cbiit-midi-dev'
location = 'us-east4'
source_dataset = 'kjsource'
destination_dataset = 'kjdestination'
source_bucket = 'midi-import/ds1_1-600/**'
destination_bucket = 'kathryntest/testfolder'

def hello_http(request):
    create_dataset(project, location, source_dataset)
    time.sleep(2)
    create_dicom_store(project, location, source_dataset, 'dicomdata')
    time.sleep(2)
    import_data (project, location, source_dataset, 'dicomdata', source_bucket)
    check_status(project, location, source_dataset, 'ImportDicomData')
    deidentify_dataset(project, location, source_dataset, destination_dataset)
    check_status(project, location, source_dataset, 'DeidentifyDataset')
    export_data (project, location, destination_dataset, 'dicomdata', destination_bucket)
    check_status(project, location, destination_dataset, 'ExportDicomData_gcs')
    return 'De-identification Complete'
