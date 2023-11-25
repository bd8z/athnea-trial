import boto3

athenaclient = boto3.client("athena")

SQLString = "CREATE EXTERNAL TABLE IF NOT EXISTS \
    table_name21 (time_ref int,\
    account string, code string, \
    country_code string, product_type string,\
    value string, status string) \
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
    ESCAPED BY '\'LINES TERMINATED BY '\n' \
    LOCATION 's3://bd8z-temporary/data-for-athena/' \
    TBLPROPERTIES ('has_encrypted_data'='false', \
    'skip.header.line.count'='1');"

response = athenaclient.start_query_execution(
    QueryString=SQLString,
    QueryExecutionContext={
        'Database': 'trial_db',
    },
    ResultConfiguration={
        'OutputLocation': 's3://bd8z-temporary/',
    },
    WorkGroup='primary'
)