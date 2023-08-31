#DELETES
import boto3

qs_client = boto3.client('quicksight')
ID= 'Accountid'

qs_client.delete_data_source(AwsAccountId = ID, DataSourceId = '')

qs_client.delete_data_set(AwsAccountId = ID, DataSetId = '')

qs_client.delete_analysis(AwsAccountId = ID, AnalysisId = '', ForceDeleteWithoutRecovery= True)