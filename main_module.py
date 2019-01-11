#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analytics API helper
Works with Reporting API v4
Some functions, which helps to work with

Need to be installed google api modules
"""
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import math
import inspect


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = None
VIEW_ID = None

body={
'reportRequests': [
{
  'viewId': VIEW_ID,
  'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'yesterday'}],
  'metrics': [{'expression': 'ga:sessions'}],
  'dimensions': [{'name': 'ga:country'}]
}]
}

class ReportSampled(Exception): pass
class CredentialsError(Exception): pass
class VIEWIDError(Exception): pass

def whoami():
    """
    Made for exceptions raising
    Returns current function name
    """
    return inspect.stack()[1][3]

def get_credentials(KEY_FILE_LOCATION):
    """Initializes an Analytics Reporting API V4 service object.
    Returns:
      An authorized Analytics Reporting API V4 service object.
    """
    if KEY_FILE_LOCATION is None:
        raise CredentialsError('credentials error in {0}'.format(whoami()))
    # get token
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)

    # build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics

def report_prettify(report, is_df = True):
    """
    Makes report more convenient
    if is_df:
        returns df
    else:
        returns dict
    """
    #get header of columns and create objects to fill
    columnHeader = report.get('columnHeader', {})
    data = {}
    dimensions = []
    metrics = []

    #creating data dict and lists of columns names
    for dimension in columnHeader.get('dimensions',[]):
        name = dimension.split(':')[1]
        dimensions.append(name)
        data[name] = []
    for metric in columnHeader['metricHeader']['metricHeaderEntries']:
        name = metric['name'].split(':')[1]
        metrics.append(name)
        data[name] = []

    # dimensions may not be, they are not required in request
    if dimensions:
        for row in report['data']['rows']:
            for i,value in enumerate(row['dimensions']):
                data[dimensions[i]].append(value)
            for i,value in enumerate(row['metrics'][0]['values']):
                data[metrics[i]].append(value)
    else:
        for row in report['data']['rows']:
            for i,value in enumerate(row['metrics'][0]['values']):
                data[metrics[i]].append(value)
    if is_df:
        return pd.DataFrame().from_dict(data)
    else:
        return data

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

def responce_to_list_reports(body,VIEW_ID,anal_cred,size=10000,list_reports=None):
    """
    Returns a list of data objects from responce
    or appends to list you have set
    """
    if  not list_reports:
        list_reports=[]
    if VIEW_ID is None:
    	raise VIEWIDError('in functioon {0}'.format(whoami()))
    page_token=0
    body['reportRequests'][0]['pageSize']=pageSize
    body['reportRequests'][0]['pageToken']=str(page_token)
    
    responce=anal_cred.reports().batchGet(body=body).execute()
    report=responce['reports'][0]
    if not is_report_sapled(report):
        num_pages = math.ceil(report['data']['rowCount'] / size)
        list_reports.append(report_prettify(report))

        for i in range(1,num_pages):
            body['reportRequests'][0]['pageToken']=str(i*size)
            responce=anal_cred.reports().batchGet(body=body).execute()
            report=responce['reports'][0]
            list_reports.append(report_prettify(report))
        return list_reports
    else:
        raise ReportSampled('Report is sampled')
