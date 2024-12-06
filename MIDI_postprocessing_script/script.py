#10/25/24 same as previous but removes curve and overlay data
#also sets media storage sop instance uid to new sop uid value




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


pydcm.config.convert_wrong_length_to_UN = True


credentials = GoogleCredentials.get_application_default()
api_version = "v1beta"
service_name = "healthcare"
service = discovery.build('healthcare', 'v1beta1', credentials=credentials)




keeplist = ['(0009, 1008)','(7005, 1008)','(7005, 100b)','(7005, 100e)','(7005, 100f)','(7005, 1012)','(7005, 1017)','(7005, 1018)','(7005, 1019)','(7005, 101a)','(7005, 101b)',
               '(7005, 101e)','(7005, 1020)','(7005, 1030)','(0019, 0014)','(0019, 10a3)','(0027, 1033)','(0043, 1035)','(0043, 1036)','(0043, 1037)','(0010, 0040)','(0010, 2203)','(0010, 1020)',
               '(0010, 1030)','(0010, 21c0)','(0010, 21a0)','(0028, 1054)','(2010, 0010)','(0008, 0070)','(0010, 1010)','(0008, 0016)','(0008, 1150)',
               '(0002, 0002)','(0002, 0010)']


file = 0
#vrs to look at
vrs = ['LO', 'SH', 'LT', 'ST', 'UT','IS']
str1 = ''
x = []
x2 = []




#regex rules
loc = re.compile('at [A-Z]{2,4}')
dr = re.compile('by [A-Z]{2}')


#phone number regex rules.
pn1 = re.compile(' (\+?)(1?)(-?)(\(?)([0-9][0-9][0-9])(\)?)(-|\.| |\))([0-9][0-9][0-9])(-|\.| )[0-9]{4}')
pn2 = re.compile(' \+([0-9]{11}|[0-9]{10})')




#blacklist
blacklist = [' MGH', ' ALH']


#whitelist of common radiology acronyms. Will probably need to be expanded
whitelist = ['CT', 'MRI', 'MR', 'CTA', 'PA', 'AP','CXR','IV','LLQ','LUQ','PET','RLQ','RUQ','US','NM','RT']


#seq = ds.OtherPatientIDsSequence


seq = [Dataset(), Dataset(), Dataset(), Dataset(), Dataset(), Dataset(), Dataset(), Dataset()]
seq[0].CodeValue = '113100'
seq[0].CodingSchemeDesignator = 'DCM'
seq[0].CodeMeaning = 'Basic Application Confidentiality Profile'


seq[1].CodeValue = '113101'
seq[1].CodingSchemeDesignator = 'DCM'
seq[1].CodeMeaning = 'Clean Pixel Data Option'


seq[2].CodeValue = '113104'
seq[2].CodingSchemeDesignator = 'DCM'
seq[2].CodeMeaning = 'Clean Structured Content Option'


seq[3].CodeValue = '113105'
seq[3].CodingSchemeDesignator = 'DCM'
seq[3].CodeMeaning = 'Clean Descriptors Option'


seq[4].CodeValue = '113107'
seq[4].CodingSchemeDesignator = 'DCM'
seq[4].CodeMeaning = 'Retain Longitudinal Temporal Information Modified Dates'


seq[5].CodeValue = '113108'
seq[5].CodingSchemeDesignator = 'DCM'
seq[5].CodeMeaning = 'Retain Patient Characteristics Option'




seq[6].CodeValue = '113111'
seq[6].CodingSchemeDesignator = 'DCM'
seq[6].CodeMeaning = 'Retain Safe Private Option'










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


def post_process(dicom, ptkeep, pid_map, i):
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
    group = ''
    pc = ''
    subset = ''
    pclist = pd.DataFrame({
        "group": [],
        "pcid": [],
        "pc": []
    })


    loc = re.compile('at ([A-Z]{2,4})')
    dr = re.compile('by ([A-Z]{2})')
    #phone number regex rules.
    pn1 = re.compile(' (\+?)(1?)(-?)(\(?)([0-9][0-9][0-9])(\)?)(-|\.| |\))([0-9][0-9][0-9])(-|\.| )[0-9]{4}')
    pn2 = re.compile(' \+([0-9]{11}|[0-9]{10})')
   
    file = dicom




    file.file_meta.add_new([0x0002, 0x0016], 'AE', "MIDI20240813ps10")
    file.add_new([0x0012, 0x0063], 'CS', 'YES')
    file.add_new([0x0012, 0x0063], 'LO', 'Per DICOM PS 2.15 AnnexE. Details in 0012,0064')
    file.add_new([0x0012, 0x0064], 'SQ', seq)
    file.file_meta[0x0002,0x0013].value = 'gcpv1beta1'
    file.file_meta[0x0002,0x0003].value = file.SOPInstanceUID
.

   
    pid = file.PatientID
   
    #creates random int between 1 and 100 using the series id as a seed to use as dateshift
    random.seed(pid)
    randdelta = random.randint(300,900)
   
   
    #creates random number between 100000-999999 to use as patient id using the current patient ID as a seed
    #might need to change this method to make it more secure
   
    newpid = pid_map['id_new'][pid_map['id_old']==pid].values[0]
    file.PatientID = str(newpid)
    file.PatientName = str(newpid)


   
    #loops through each element in the dicom file
    for elem in file.iterall():
        if str(elem.name) == "Patient's Birth Date":
            elem.value = ''
            continue
        if str(elem.name) == 'Clinical Trial Subject ID' or str(elem.name) == 'Clinical Trial Subject Reading ID':
            elem.value = newpid
            continue
        if str(elem.value) == 'PLACEHOLDER' or str(elem.name) == 'Verifying Observer Name' or str(elem.name) == 'Person Name':
            elem.value = 'REMOVED'
            continue
        if elem.VR == 'SQ':
            continue


        if str(elem.name) == 'Private Creator':
            pc  = str(elem.value)
            tag = str(elem.tag)
            group = tag[1:5].upper()
            pcid = tag[9:11]
            pclist.loc[len(pclist)] = [group, pcid, pc]
            continue
        #removes private data elements
        tag = str(elem.tag)
        groupcur = tag[1:5].upper()
        if groupcur in pclist['group'].values:
            pcidcur = tag[7:9].upper()
            pccur = pclist[(pclist['group']==groupcur) & (pclist['pcid']==pcidcur)]['pc'].values[0]
           
            subset_owner = ptkeep[ptkeep['owner']==pccur]
            subset = subset_owner[subset_owner['group']==groupcur]
            if str(elem.name) == 'Private Creator' or elem.name == 'Actual Frame Duration' or elem.name == 'Primary (Prompts) Counts Accumulated' or elem.name == 'Pixel Data':
                continue
            lasttwo = tag[9:11]
            if (subset['element'].eq(lasttwo.upper())).any():
                continue
            else:
                elem.value = ''
                continue
        if tag[1:3] == '60' or tag[1:3] == '50':
            elem.value = ''
               
        #removes text value
        if elem.name == 'Text Value':
                 elem.value = '1'
       
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
                #shifts the date back 300-900 days using the previously created random number
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
                ilist.append(i)
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
                ilist.append(i)
                taglist.append(elem.name)
            elem.value = str1[1:len(str1)-1]
            newlist.append(str1[1:len(str1)-1])
           
        #ignores elements not of VR 'LO', 'SH', 'LT', 'ST', 'UT','IS'
        #ignores Patient ID, Code Value, Private Creator, Actual Frame Duration, and Primary Counts Accumulates elements
        elif elem.VR in vrs and elem.name != 'Patient ID' and elem.name != 'Code Value' and elem.name != 'Private Creator' and elem.name != 'Actual Frame Duration' and elem.name != 'Primary (Prompts) Counts Accumulated':
            #ignores tags in automatic keep list
            if str(elem.tag).upper() not in keeplist:
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
                        ilist.append(i)
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
                        ilist.append(i)
                        orglist.append(str1)
                        orgtextlist.append(matchdr.group(0))
                        str1 = strdr
                        newlist.append(str1)
                        newtextlist.append('1')
                        elem.value = str1
                #looks for phone number format number 1
                matchpn1 = pn1.search(str1)
                if matchpn1 is not None:
                    strpn1 = pn1.sub('1',str1)
                    #the append statements are just for creating a df to double check results, not a part of the de-id process
                   
                    str1 = strpn1
                    #updates elem value with new string
                    elem.value = str1
                #looks for phone number format number 2
                matchpn2 = pn2.search(str1)
                if matchpn2 is not None:
                    strpn2 = pn2.sub('1',str1)
                    #the append statements are just for creating a df to double check results, not a part of the de-id process
                   
                    str1 = strpn2
                    #updates elem value with new string
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
                                ilist.append(i)
                               
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
                                            ilist.append(i)
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






   





