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

try:
    import get_data_from_google_bigquery
except ModuleNotFoundError:
    sys.path.append(os.path.abspath(os.path.dirname(__file__)) + '/../google_analytics')
    import get_data_from_google_bigquery

def __check_conf_data(conf_data: dict) -> bool:
    """check conf data."""
    # required
    if (
        ('google_analytics' not in conf_data) or
        ('google_bigquery' not in conf_data) or
        ('data_from' not in conf_data['google_analytics']) or
        ('tid' not in conf_data['google_analytics']) or
        ('project_id' not in conf_data['google_bigquery']) or
        ('dataset_id' not in conf_data['google_bigquery']) or
        ('table_id' not in conf_data['google_bigquery']) or
        (conf_data['google_analytics']['tid'] is None) or
        (conf_data['google_bigquery']['project_id'] is None) or
        (conf_data['google_bigquery']['dataset_id'] is None) or
        (conf_data['google_bigquery']['table_id'] is None)
    ):
        return False

    # value
    if (
        (conf_data['google_analytics']['data_from'] != '1') and
        (conf_data['google_analytics']['data_from'] != '2')
    ):
        return False

    # related
    if conf_data['google_analytics']['data_from'] == '1':
        # get data from GA
        if (
            ('ga' not in conf_data['google_analytics']) or
            ('view_id' not in conf_data['google_analytics']['ga']) or
            ('client_id_dimension' not in conf_data['google_analytics']['ga']) or
            ('app_id_dimension' not in conf_data['google_analytics']['ga']) or
            ('start_date' not in conf_data['google_analytics']['ga']) or
            ('end_date' not in conf_data['google_analytics']['ga']) or
            (conf_data['google_analytics']['ga']['view_id'] is None) or
            (conf_data['google_analytics']['ga']['client_id_dimension'] is None) or
            (conf_data['google_analytics']['ga']['app_id_dimension'] is None) or
            (conf_data['google_analytics']['ga']['start_date'] is None) or
            (conf_data['google_analytics']['ga']['end_date'] is None)
        ):
            return False
    if conf_data['google_analytics']['data_from'] == '2':
        # get data from BQ
        if (
            ('bq' not in conf_data['google_analytics']) or
            ('project_id' not in conf_data['google_analytics']['bq']) or
            ('dataset_id' not in conf_data['google_analytics']['bq']) or
            ('client_id_dimension' not in conf_data['google_analytics']['bq']) or
            ('app_id_dimension' not in conf_data['google_analytics']['bq']) or
            (conf_data['google_analytics']['bq']['project_id'] is None) or
            (conf_data['google_analytics']['bq']['dataset_id'] is None) or
            (conf_data['google_analytics']['bq']['client_id_dimension'] is None) or
            (conf_data['google_analytics']['bq']['app_id_dimension'] is None)
        ):
            return False

    return True


if __name__ == '__main__':
    print('%s %s start.' % (datetime.today(), __file__))

    # get parameters
    args = docopt(__doc__)
    conf_file_path = args['--conf_file_path']
    output_dir_path = args['--output_dir_path']

    # config
    with open(conf_file_path) as f:
        conf_data = yaml.load(f)

    # check conf data
    if __check_conf_data(conf_data) is False:
        print('conf error.')
        sys.exit(1)

    # data from
    tid = conf_data['google_analytics']['tid']
    data_from = conf_data['google_analytics']['data_from']
    if data_from == '1':
        # GA
        view_id = conf_data['google_analytics']['ga']['view_id']
        client_id_dimension = conf_data['google_analytics']['ga']['client_id_dimension']
        app_id_dimension = conf_data['google_analytics']['ga']['app_id_dimension']
        start_date = conf_data['google_analytics']['ga']['start_date']
        end_date = conf_data['google_analytics']['ga']['end_date']

        # output file
        csv_file_path = output_dir_path + ('%s_%s.csv' % (tid, view_id))

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

    else:
        # BQ
        project_id = conf_data['google_analytics']['bq']['project_id']
        dataset_id = conf_data['google_analytics']['bq']['dataset_id']
        client_id_dimension = conf_data['google_analytics']['bq']['client_id_dimension']
        app_id_dimension = conf_data['google_analytics']['bq']['app_id_dimension']

        # output file
        csv_file_path = output_dir_path + ('%s_%s.csv' % (tid, dataset_id))

        # create model
        gdfgb = get_data_from_google_bigquery.GetDataFromGoogleBigQuery(project_id)

        # get data
        data = gdfgb.get_data(
            dataset_id,
            client_id_dimension,
            app_id_dimension
        )

    # data to
    # BQ
    project_id = conf_data['google_bigquery']['project_id']
    dataset_id = conf_data['google_bigquery']['dataset_id']
    table_id = conf_data['google_bigquery']['table_id']

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
    # set up
    bigquery_client = bigquery.Client(project=project_id)
    dataset = bigquery_client.dataset(dataset_id)
    table = dataset.table(table_id)

    # check table
    table_list = bigquery_client.list_dataset_tables(dataset)
    table_exists_flg = 0
    for existed_table in table_list:
        if existed_table.table_id == table_id:
            table_exists_flg = 1
            break

    if table_exists_flg == 0:
        # create table
        schema = [
            bigquery.SchemaField('tid', 'STRING', mode='required'),
            bigquery.SchemaField('client_id', 'STRING', mode='required'),
            bigquery.SchemaField('application_id', 'STRING', mode='required'),
            bigquery.SchemaField('created_at', 'TIMESTAMP', mode='required'),
        ]
        table_ref = bigquery.Table(table, schema=schema)
        bigquery_client.create_table(table_ref)

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
