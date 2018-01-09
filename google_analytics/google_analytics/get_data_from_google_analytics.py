#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""Get Google Analytics data using Reporting API v4."""

from apiclient.discovery import build
from apiclient.discovery import Resource
import argparse
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import os

class GetDataFromGoogleAnalytics(object):
    """Get Google Analytics data using Reporting API v4."""

    def __init__(
        self,
        view_id: str,
        client_id_dimension: str,
        app_id_dimension: str,
        start_date: str,
        end_date: str,
    ):
        """init."""
        self.view_id = view_id
        self.client_id_dimension = client_id_dimension
        self.app_id_dimension = app_id_dimension
        self.start_date = start_date
        self.end_date = end_date
        self.scopes = ['https://www.googleapis.com/auth/analytics.readonly']
        self.discovery_uri = ('https://analyticsreporting.googleapis.com/$discovery/rest')
        self.config_dir_path = os.path.abspath(os.path.dirname(__file__)) + '/../config/'
        self.client_secrets_path = self.config_dir_path + 'client_secrets.json'

    def initialize_analyticsreporting(self) -> Resource:
        """Parse command-line arguments."""
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            parents=[tools.argparser]
        )
        flags = parser.parse_args([])

        # Set up a Flow object to be used if we need to authenticate.
        flow = client.flow_from_clientsecrets(
            self.client_secrets_path,
            scope=self.scopes,
            message=tools.message_if_missing(self.client_secrets_path)
        )

        """
        Prepare credentials, and authorize HTTP object with them.
        If the credentials don't exist or are invalid run through the native client
        flow. The Storage object will ensure that if successful the good
        credentials will get written back to a file.
        """
        storage = file.Storage(self.config_dir_path + 'analyticsreporting.dat')
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, flags)
        http = credentials.authorize(http=httplib2.Http())

        # Build the service object.
        analytics = build(
            'analytics',
            'v4',
            http=http,
            discoveryServiceUrl=self.discovery_uri
        )

        return analytics

    def get_report(
        self,
        analytics: Resource,
    ) -> dict:
        """Use the Analytics Service Object to query the Analytics Reporting API V4."""
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': self.view_id,
                        'dateRanges': [
                            {
                                'startDate': self.start_date,
                                'endDate': self.end_date,
                            }
                        ],
                        'metrics': [{'expression': 'ga:users'}],
                        'dimensions': [
                            {'name': self.app_id_dimension},
                            {'name': self.client_id_dimension},
                        ],
                        'samplingLevel': 'LARGE',
                    }
                ]
            }
        ).execute()

    def response_to_list(
        self,
        response: dict,
    ) -> list:
        """Parse response from Analytics Reporting API V4."""
        for report in response.get('reports', []):
            column_header = report.get('columnHeader', {})
            dimension_headers = column_header.get('dimensions', [])
            rows = report.get('data', {}).get('rows', [])

        ret = []
        for row in rows:
            tmp = {}
            dimensions = row.get('dimensions', [])

            for header, dimension in zip(dimension_headers, dimensions):
                if header == self.client_id_dimension:
                    tmp['client_id'] = dimension
                else:
                    tmp['application_id'] = dimension

            ret.append(tmp)

        return ret
