import re

import boto3
import pandas as pd


class createAthenaTalbe:
    def __init__(self, csvFilePath, tableName, s3QueryLogPath):
        self.df = pd.read_csv(csvFilePath)
        self.tableName = tableName
        self.s3QueryLogPath = s3QueryLogPath
    def createSQLString(self) ->None:
        colList = self.df.columns.to_list()
        SQLString = []
        for col in colList:
            col_ = "`" + col + "`"
            if "float" in str(self.df[col].dtype):
                SQLString.append(col_ +" float,")
            elif "int" in str(self.df[col].dtype):
                SQLString.append(col_ +" int,")
            elif re.match(r'([0-9]{4}-[0-9]{2}-[0-9]{2}.\
                        [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})', self.df["status"][1]) :
                SQLString.append( + col_ + " date,")
            else:
                SQLString.append(col_ + " string,")
        self.sqlMEssate =  "CREATE EXTERNAL TABLE `" + self.tableName + "` ( " + " ".join(SQLString)[:-1] \
            + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' LOCATION '" \
            + self.s3QueryLogPath + "' TBLPROPERTIES ( 'has_encrypted_data'='false', 'skip.header.line.count'='1' )" 
        
    def startQuery(self) ->str:
        athenaclient = boto3.client("athena")
        response = athenaclient.start_query_execution(
            QueryString=self.sqlMEssate,
            QueryExecutionContext={
                'Database': 'trial_db',
            },
            ResultConfiguration={
                'OutputLocation': 's3://bd8z-temporary/',
            },
            WorkGroup='primary'
        )
        return response["QueryExecutionId"]
    
if __name__ == "__main__":
    athObj = createAthenaTalbe("output_csv_full.csv","new_talbe_a","s3://bd8z-temporary/data-for-athena/")
    athObj.createSQLString()
    athObj.startQuery()