#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""Get Google Analytics data using Reporting API v4."""

import os
import sys
import unittest

try:
    import get_data_from_google_analytics
except ModuleNotFoundError:
    sys.path.append(os.path.abspath(os.path.dirname(__file__)) + '/../google_analytics')
    import get_data_from_google_analytics

class GetDataFromGoogleAnalyticsTests(unittest.TestCase):
    """Get Google Analytics data using Reporting API v4."""

    def setUp(self):
        """set up."""
        self.view_id = 'view_id'
        self.client_id_dimension = 'client_id_dimension'
        self.app_id_dimension = 'app_id_dimension'
        self.start_date = 'yesterday'
        self.end_date = 'yesterday'

        # create model
        self.gdfga = get_data_from_google_analytics.GetDataFromGoogleAnalytics(
            self.view_id,
            self.client_id_dimension,
            self.app_id_dimension,
            self.start_date,
            self.end_date
        )

    def test_init(self):
        """init."""
        self.assertEqual(self.gdfga.view_id, self.view_id)
        self.assertEqual(self.gdfga.client_id_dimension, self.client_id_dimension)
        self.assertEqual(self.gdfga.app_id_dimension, self.app_id_dimension)
        self.assertEqual(self.gdfga.start_date, self.start_date)
        self.assertEqual(self.gdfga.end_date, self.end_date)

    def test_response_to_list(self):
        """Parse response from Analytics Reporting API V4."""
        input_dict = {
            'reports': [
                {
                    'columnHeader': {
                        'dimensions': [
                            self.app_id_dimension,
                            self.client_id_dimension,
                        ],
                    },
                    'data': {
                        'rows': [
                            {
                                'dimensions': [
                                    'application_id_1',
                                    'client_id_1',
                                ],
                            },
                            {
                                'dimensions': [
                                    'application_id_2',
                                    'client_id_2',
                                ],
                            },
                            {
                                'dimensions': [
                                    'application_id_3',
                                    'client_id_3',
                                ],
                            },
                        ],
                    },
                }
            ],
        }
        output_list = [
            {'application_id': 'application_id_1', 'client_id': 'client_id_1'},
            {'application_id': 'application_id_2', 'client_id': 'client_id_2'},
            {'application_id': 'application_id_3', 'client_id': 'client_id_3'},
        ]
        self.assertEqual(self.gdfga.response_to_list(input_dict), output_list)
