#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analytics API helper
Some functions, which helps to work with

Need to be installed google api modules
"""
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import math


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'KEY_FILE_LOCATION'
VIEW_ID = 'VIEW_ID'


class ReportSampled(Exception): pass

def get_credentials():
    """Initializes an Analytics Reporting API V4 service object.
    Returns:
      An authorized Analytics Reporting API V4 service object.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics

def report_to_dataframe(report):    
    """
    Converts peace of analytics report in data frame.
    """
    columns=[] 
    columnHeader=report.get('columnHeader', {})
    for dimension in columnHeader['dimensions']:
        columns.append(dimension)
    for metric in columnHeader['metricHeader']['metricHeaderEntries']:
        columns.append(metric.get('name'))
    
    rows_to_add={}
    count=0
    for row in report['data']['rows']:
        row_to_add={}
        len_dimensions=len(columnHeader['dimensions'])
        len_metrics=len(columnHeader['metricHeader']['metricHeaderEntries'])
        for i in range(len_dimensions):
           row_to_add[columns[i]]=row['dimensions'][i]
        for i in range(len_metrics):
            row_to_add[columns[i+len_dimensions]]=row['metrics'][0]['values'][i]
        rows_to_add[count]=row_to_add
        count+=1
    df=pd.DataFrame().from_dict(rows_to_add, orient='index')
    return df

def is_report_sapled(report):
    """
    Returns True if report sampled, else returns False
    """
    keys=report['data'].keys()
    flag=False
    for key in keys:
        if (key == 'samplingSpaceSizes') or (key == 'samplesReadCounts'):
            flag=True
    return flag

def report_to_list_dfS(body,anal_cred,size=10000,list_df=None):
    """
    Returns a list of data frames from a whole analytics report
    """
    if  not list_df:
        list_df=[]
    page_token=0
    body['reportRequests'][0]['pageSize']=size
    body['reportRequests'][0]['pageToken']=str(page_token)
    
    responce=anal_cred.reports().batchGet(body=body).execute()
    report=responce['reports'][0]
    if not is_report_sapled(report):
        num_pages = math.ceil(report['data']['rowCount'] / size)
        list_df.append(report_to_dataframe(report))

        for i in range(1,num_pages):
            body['reportRequests'][0]['pageToken']=str(i*size)
            responce=anal_cred.reports().batchGet(body=body).execute()
            report=responce['reports'][0]
            list_df.append(report_to_dataframe(report))
        return list_df
    else:
        raise ReportSampled('Report is sampled')
