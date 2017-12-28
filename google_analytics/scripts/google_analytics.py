#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""Get Google Analytics data using Reporting API v4.

Usage:
    google_analytics.py
        --conf_file_path=<conf_file_path>
        --output_dir_path=<output_dir_path>
    google_analytics.py -h | --help
Options:
    -h --help  show this screen and exit.
"""

from datetime import datetime
from docopt import docopt
from google.cloud import bigquery
import os
import pandas as pd
import sys
import yaml

try:
    import get_data_from_google_analytics
except ModuleNotFoundError:
    sys.path.append(os.path.abspath(os.path.dirname(__file__)) + '/../google_analytics')
    import get_data_from_google_analytics

if __name__ == '__main__':
    print('%s %s start.' % (datetime.today(), __file__))

    # get parameters
    args = docopt(__doc__)
    conf_file_path = args['--conf_file_path']
    output_dir_path = args['--output_dir_path']

    # config
    with open(conf_file_path) as f:
        conf_data = yaml.load(f)
    # GA
    tid = conf_data['google_analytics']['tid']
    view_id = conf_data['google_analytics']['view_id']
    client_id_dimension = conf_data['google_analytics']['client_id_dimension']
    app_id_dimension = conf_data['google_analytics']['app_id_dimension']
    start_date = conf_data['google_analytics']['start_date']
    end_date = conf_data['google_analytics']['end_date']
    # BQ
    project_id = conf_data['google_bigquery']['project_id']
    dataset_id = conf_data['google_bigquery']['dataset_id']
    table_id = conf_data['google_bigquery']['table_id']
    # file
    csv_file_path = output_dir_path + 'app_id_client_id.csv'

    # create model
    gdfga = get_data_from_google_analytics.GetDataFromGoogleAnalytics(
        view_id,
        client_id_dimension,
        app_id_dimension,
        start_date,
        end_date
    )

    # get data
    analytics = gdfga.initialize_analyticsreporting()
    response = gdfga.get_report(analytics)
    data = gdfga.response_to_list(response)

    # to csv
    df = pd.DataFrame.from_dict(data)
    df['tid'] = pd.Series(tid, index=df.index)
    today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    df['created_at'] = pd.Series(today, index=df.index)
    # sort
    df = df.ix[
        :,
        [
            'tid',
            'client_id',
            'application_id',
            'created_at',
        ]
    ]
    # write
    df.to_csv(csv_file_path, index=False, header=True)

    # send BigQuery
    bigquery_client = bigquery.Client(project=project_id)
    dataset = bigquery_client.dataset(dataset_id)
    table = dataset.table(table_id)
    # config
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'CSV'
    job_config.skip_leading_rows = 1
    job_config.allow_quoted_newlines = True
    # bq load
    with open(csv_file_path, 'rb') as readable:
        bigquery_client.load_table_from_file(
            readable,
            table,
            job_config=job_config
        )

    print('%s %s end.' % (datetime.today(), __file__))
    sys.exit(0)
