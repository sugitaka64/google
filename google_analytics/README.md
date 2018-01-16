# google_analytics

## 概要

[Googleアナリティクス](https://analytics.google.com/)の特定のディメンションを[Google BigQuery](https://bigquery.cloud.google.com/)に保存するプログラムです。
GoogleアナリティクスのデータをGoogle BigQueryに[エクスポート](https://support.google.com/analytics/answer/3437618)している場合、そちらのデータセットからもデータを取得可能です。

## 事前準備

1. Googleアナリティクス、Google BigQueryのアカウントを作成ください。
2. Google BigQueryに適当なデータセットを作成してください。
3. このプログラムを実行する環境にpipenvをインストールしてください。

```shell-session
$ pip install pipenv
```

4. 本プログラムをcloneし、設定ファイルを作成してください。

```shell-session
$ git clone https://github.com/sugitaka64/google.git
$ cd google/google_analytics
$ pipenv update
$ cp config/conf_dummy.yml config/conf.yml
$ vi config/conf.yml
```

以下、設定ファイルの説明です。

* google_analytics
  * data_from            : 1:Googleアナリティクスからデータ取得、2:Google BigQueryからデータ取得、それ以外の値はエラー
  * tid                  : GoogleアナリティクスのトラッキングID
  * ga                   : Googleアナリティクスの設定
    * view_id            :   GoogleアナリティクスのビューID
    * client_id_dimension:   取得するGoogleアナリティクスのカスタムディメンション名
    * app_id_dimension   :   取得するGoogleアナリティクスのカスタムディメンション名
    * start_date         :   取得開始日（`yesterday`、`7daysAgo`、`2017-01-01`等）
    * end_date           :   取得終了日（同上）
  * bq                   : 取得先のGoogle BigQueryの設定
    * project_id         :   Google BigQueryのプロジェクトID
    * dataset_id         :   Google BigQueryのデータセットID
    * client_id_dimension:   取得するGoogleアナリティクスのカスタムディメンションインデックス
    * app_id_dimension   :   取得するGoogleアナリティクスのカスタムディメンションインデックス
* google_bigquery        : 保存先のGoogle BigQueryの設定
  * project_id           :   Google BigQueryのプロジェクトID
  * dataset_id           :   Google BigQueryのデータセットID
  * table_id             :   Google BigQueryのテーブルID

## 使用方法

```shell-session
$ pipenv run python scripts/google_analytics.py \
  --conf_file_path=./config/conf.yml \
  --output_dir_path=./outputs/
```

`output_dir_path`に指定したディレクトリ内に、取得したデータがCSV形式で出力されます。  
また、設定ファイルで設定したBigQueryテーブルに同様のデータが格納されます。  
（テーブルが無い場合は作成され、ある場合はデータ追加になります。）

## 出力ファイル説明

* app_id_client_id.csv
  * tid           : google_analytics.tidで設定した値
  * client_id     : google_analytics.client_id_dimensionで設定したカスタムディメンションのデータ
  * application_id: google_analytics.app_id_dimensionで設定したカスタムディメンションのデータ
  * created_at    : データ作成日

## BigQueryテーブル説明

* app_id_client_id 
  * 各カラムの内容は、出力ファイルと同様なのでそちらを参照

## unittest

```shell-session
$ pipenv run python -m unittest tests/*.py
```
