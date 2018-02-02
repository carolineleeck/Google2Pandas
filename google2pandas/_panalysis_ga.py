

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from sys import stdout
from copy import deepcopy

import pandas as pd
import numpy as np
import httplib2, os

from ._query_parser import QueryParser

SCOPES = 'https://www.googleapis.com/auth/analytics.readonly'
default_discovery = 'https://analyticsreporting.googleapis.com/$discovery/rest'


class GoogleServiceReader(object):
    '''
    Abstract class for handling OAuth2 authentication using the Google
    oauth2client library and the V4 Analytics API
    '''
    def __init__(self, scope, discovery_uri):
        '''
        Parameters:
        -----------
            secrets : string
                Path to client_secrets.json file. p12 formatted keys not
                supported at this point.
            scope : list or string
                Designates the authentication scope(s).
            discovery_uri : tuple or string
                Designates discovery uri(s)
        '''
        self._scope_ = scope
        self._discovery_ = discovery_uri
        self._api_ = 'v4'

    def _init_service(self, credentials):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials, SCOPES)

        analytics = build(
            'analyticsreporting', 
            'v4', 
            credentials=credentials)  
        return analytics                   


class GoogleAnalyticsQuery(GoogleServiceReader):
    def __init__(self, secrets_location):
        '''
        Query the GA API with ease!  Simply pass the service credentials file

        At the very least, the 'fields' parameter should be included here:

            https://developers.google.com/analytics/devguides/reporting/core/v4/parameters
        '''
        
        self._service = self._init_service(secrets_location)

    def execute_query(self, query, as_dict=False, all_results=True):
        '''
        Execute **query and translate it to a pandas.DataFrame object.

        Parameters:
        -----------
            query: dict
                Refer to:

                    https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports

                for guidance. Automatic parsing has been deprecated in V4.
            as_dict : Boolean
                Return the dict object provided by GA instead of the DataFrame
                object. Default = False
            all_results : Boolean
                Get all the data for the query instead of the 1000-row limit.
                Defualt = True

        Returns:
        -----------
            df : pandas.DataFrame
                Reformatted response to **query.
        '''
        if all_results:
            qry = deepcopy(query)
            out = {'reports' : []}

            while True:
                response = self._service.reports().batchGet(body=qry).execute()
                out['reports'] = out['reports'] + response['reports']

                tkn = response.get('reports', [])[0].get('nextPageToken', '')
                if tkn:
                    qry['reportRequests'][0].update({'pageToken' : tkn})

                else:
                    break

        else:
            out = self._service.reports().batchGet(body=query).execute()

        if as_dict:
            return out

        else:
            return self.resp2frame(out)

    @staticmethod
    def resp2frame(resp):
        # return object
        out = pd.DataFrame()
        # GA data type to data frame conversion
        lookup = {
          "INTEGER": "int32",
          "FLOAT": "float32",
          "CURRENCY": "float32",
          "PERCENT": "float32",
          "TIME": "object",
          "STRING": "object"
        }

        # Loop through reports and get metrics and dimensions
        for report in resp.get('reports', []):
            col_hdrs = report.get('columnHeader', {})
            # Get the initial dimensions
            cols = col_hdrs['dimensions']
            metric_cols = []
            if 'metricHeader' in list(col_hdrs.keys()):
                metrics = col_hdrs.get('metricHeader', {}).get('metricHeaderEntries', [])
                cols_data_type = {}
                for m in metrics:
                    # Get each metric and the data type
                    cols = cols + [m.get('name')]
                    cols_data_type[m.get('name')] = lookup[m.get('type')]

            # Take out any "ga:" prefixes
            cols = list(map(lambda x: x.replace("ga:", ""), cols))
            # Set the dataframe with the column names
            df = pd.DataFrame(columns=cols)
            # Get the rows from the GA report
            rows = report.get('data', {}).get('rows')
            # Let's loop through the rows to get the dimensions and metrics to row list
            for row in rows:
                row_list = row.get('dimensions', [])

                if 'metrics' in list(row.keys()):
                    metrics = row.get('metrics', [])
                    for m in metrics:
                        row_list = row_list + m.get('values')

                # Make each row an enumerated dictionary with index value starting
                # at 0
                drow = {}
                for i, c in enumerate(cols):
                    drow.update({c : row_list[i]})

                # Concatanate the row to the overall list
                df = pd.concat((df, pd.DataFrame(drow, index=[0])),
                               ignore_index=True)

            # Copy the dataframe to the returning object
            out = pd.concat((out, df), ignore_index=True)
            # Convert the object types to the inferred ones
            out = out.apply(pd.to_numeric, errors='ignore', axis=1)
            # Explicitly convert date back to a date object
            if 'date' in out.columns:
                out['date'] = pd.to_datetime(out['date'], format="%Y%m%d")

        return out