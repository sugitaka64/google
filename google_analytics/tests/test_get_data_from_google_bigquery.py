#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""Get Google Analytics data from Google BigQuery."""

from google.cloud.bigquery.client import Client
import os
import sys
import unittest

try:
    import get_data_from_google_bigquery
except ModuleNotFoundError:
    sys.path.append(os.path.abspath(os.path.dirname(__file__)) + '/../google_analytics')
    import get_data_from_google_bigquery

class GetDataFromGoogleBigQueryTests(unittest.TestCase):
    """Get Google Analytics data from Google BigQuery."""

    def setUp(self):
        """set up."""
        self.project_id = 'project_id'

        # create model
        self.gdfgb = get_data_from_google_bigquery.GetDataFromGoogleBigQuery(
            self.project_id,
        )

    def test_init(self):
        """init."""
        self.assertTrue(isinstance(self.gdfgb.bigquery_client, Client))
