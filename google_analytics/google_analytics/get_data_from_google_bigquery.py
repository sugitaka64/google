#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""Get Google Analytics data from Google BigQuery."""

from datetime import date
from datetime import timedelta
from google.cloud import bigquery

class GetDataFromGoogleBigQuery(object):
    """Get Google Analytics data from Google BigQuery."""

    def __init__(
        self,
        project_id: str,
    ):
        """init."""
        self.bigquery_client = bigquery.Client(project=project_id)

    def get_data(
        self,
        dataset_id: str,
        client_id_clumn: str,
        app_id_clumn: str,
    ) -> dict:
        """make query and get records from BigQuery."""
        target_date = (date.today() - timedelta(1)).strftime('%Y%m%d')
        query = (
            'SELECT '
            '  (SELECT MAX(IF(index=%s, value, NULL)) '
            '    FROM UNNEST(unnest_hits.customDimensions)) AS application_id, '
            '  (SELECT MAX(IF(index=%s, value, NULL)) '
            '    FROM UNNEST(unnest_hits.customDimensions)) AS client_id '
            'FROM '
            '  `%s.ga_sessions_*` '
            'INNER JOIN '
            '  UNNEST(hits) AS unnest_hits '
            'WHERE '
            '  _TABLE_SUFFIX BETWEEN \'%s\' AND \'%s\' '
            '  AND (SELECT MAX(IF(index=%s, value, NULL)) '
            '    FROM UNNEST(unnest_hits.customDimensions)) IS NOT NULL '
            '  AND (SELECT MAX(IF(index=%s, value, NULL)) '
            '    FROM UNNEST(unnest_hits.customDimensions)) IS NOT NULL '
            'GROUP BY '
            '  application_id, client_id;'
        ) % (
            app_id_clumn,
            client_id_clumn,
            dataset_id,
            target_date,
            target_date,
            app_id_clumn,
            client_id_clumn
        )
        query_job = self.bigquery_client.query(query)
        rows = query_job.result()

        # make dict
        ret = []
        for row in rows:
            tmp = {}
            tmp['client_id'] = row.client_id
            tmp['application_id'] = row.application_id
            ret.append(tmp)

        return ret
