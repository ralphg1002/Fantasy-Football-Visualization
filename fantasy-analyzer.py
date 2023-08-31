import boto3
import uuid
import json

def create_s3Bucket(bucket_name, region=None):
    if region is None:
        bucket = s3_resource.create_bucket(
            Bucket= bucket_name,
            ObjectOwnership = 'BucketOwnerEnforced'    
        )
    else:
        bucket = s3_resource.create_bucket(
            Bucket = bucket_name,
            ObjectOwnership = 'BucketOwnerEnforced', #Important: allows to upload files
            CreateBucketConfiguration = {'LocationConstraint': region}
        )
            
    print(bucket)

#Allow ability to grant public access to objects (through ACLs)
def allow_public_access(bucket_name):
    #The follow lines of code perform the same actions
    # s3_client.delete_public_access_block(Bucket=bucket_name)
    
    #However, this is allows for more control over public policies in the bucket
    s3_client.put_public_access_block(
        Bucket=bucket_name, 
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': False,
            'IgnorePublicAcls': False,
            'BlockPublicPolicy': False,
            'RestrictPublicBuckets': False
        }
    )

#Configure Bucket policy 
def configure_bucket_policy(bucket_name, policy_file):
    bucket_policy = s3_resource.BucketPolicy(bucket_name)
    with open(policy_file,"r") as file:
        json_file = json.load(file) #Read in as a python dictionary
        json_policy = json.dumps(json_file) #Must convert to json object
        bucket_policy.put(
            Policy = json_policy
        )
    print(json_file)

def upload_file(bucket_name, path, key):
    bucket = s3_resource.Bucket(bucket_name)
    bucket.upload_file(
        Filename = path, 
        Key = key
    )

# #Upload s3 files to quicksight
def s3_to_qsdatasource(account_id, datasource_id, bucket_name, datasource_name):

    #Imported from json file
    source_params = "datasource-params.json"
    
    with open(source_params,"r") as file:
        datasource_params = json.load(file)
        datasource_params["S3Parameters"]["ManifestFileLocation"]["Bucket"] = bucket_name
    
    qs_client.create_data_source(
        AwsAccountId = account_id,
        DataSourceId = datasource_id,
        Name = datasource_name,
        Type = 'S3',
        #Accesses manifest file that points to the correct csv uri
        DataSourceParameters = datasource_params,
        Permissions = [
            {
                'Principal' : '<arn>',
                'Actions' : [
                    'quicksight:DescribeDataSource',
                    'quicksight:DescribeDataSourcePermissions',
                    'quicksight:PassDataSource',
                    'quicksight:UpdateDataSource',
                    'quicksight:UpdateDataSourcePermissions',
                    'quicksight:DeleteDataSource'
                ]
            }
        ]
    )
    
    return datasource_id

def qsdatasource_to_qsdataset(account_id, dataset_id, dataset_name, pmap_id, lmap_id, datasource_id):

    print(datasource_id)
    physicalmap = "dataset-physical-map.json"
    logicalmap = "dataset-logical-map.json"
    
    with open(physicalmap,"r") as file:
        dataset_physical_map = json.load(file)
        
        #Modify field for dataset to create from intended datasource
        new_datasourceARN = dataset_physical_map["S3Source"]["DataSourceArn"] + datasource_id
        dataset_physical_map["S3Source"]["DataSourceArn"] = new_datasourceARN
        
    with open(logicalmap,"r") as file:
        dataset_logical_map = json.load(file)
        dataset_logical_map["Alias"] = dataset_name
        dataset_logical_map["Source"]["PhysicalTableId"] = pmap_id
    
    qs_client.create_data_set(
        AwsAccountId = account_id,
        DataSetId = dataset_id,
        Name = dataset_name,
        PhysicalTableMap = {
            pmap_id: dataset_physical_map
        },
        LogicalTableMap = {
            lmap_id: dataset_logical_map
        },
        ImportMode = 'SPICE',
        Permissions = [
            {
                'Principal' : '<arn>',
                'Actions' : [
                    'quicksight:DescribeDataSet',
                    'quicksight:DescribeDataSetPermissions',
                    'quicksight:PassDataSet',
                    'quicksight:DeleteDataSet',
                    'quicksight:UpdateDataSetPermissions',
                    'quicksight:PutDataSetRefreshProperties',
                    'quicksight:CreateRefreshSchedule',
                    'quicksight:CancelIngestion',
                    'quicksight:UpdateRefreshSchedule',
                    'quicksight:DeleteRefreshSchedule',
                    'quicksight:ListRefreshSchedules',
                    'quicksight:DescribeDataSetRefreshProperties',
                    'quicksight:CreateIngestion',
                    'quicksight:DescribeRefreshSchedule',
                    'quicksight:ListIngestions',
                    'quicksight:UpdateDataSet',
                    'quicksight:DeleteDataSetRefreshProperties',
                    'quicksight:DescribeIngestion'
                ]
            }
        ]
    )
    
    return dataset_id, dataset_name

def qsdataset_to_analysis(account_id, analysis_id, analysis_name, sheet_id, sheet_name, dataset_name, dataset_id):
    
    print(dataset_id)
    print(analysis_id)
    definition = "analysis-definition.json"
    
    with open(definition,"r") as file:
        analysis_definition = json.load(file)
        
        analysis_definition["DataSetIdentifierDeclarations"][0]["Identifier"] = dataset_name
        analysis_definition["Sheets"][0]["SheetId"] = sheet_id
        analysis_definition["Sheets"][0]["Name"] = sheet_name
        analysis_definition["Sheets"][0]["Visuals"][0]["LineChartVisual"]["ChartConfiguration"]["FieldWells"]["LineChartAggregatedFieldWells"]["Category"][0]["CategoricalDimensionField"]["Column"]["DataSetIdentifier"] = dataset_name
        
        #Modify field for analysis to create from intended dataset
        new_datasetARN = analysis_definition["DataSetIdentifierDeclarations"][0]["DataSetArn"] + dataset_id
        analysis_definition["DataSetIdentifierDeclarations"][0]["DataSetArn"] = new_datasetARN
        
    qs_client.create_analysis(
        AwsAccountId = account_id,
        AnalysisId = analysis_id,
        Name = analysis_name,
        ThemeArn = 'arn:aws:quicksight::aws:theme/CLASSIC',
        Definition = analysis_definition,
        Permissions = [
            {
                'Principal' : '<arn>',
                'Actions' : [
                    'quicksight:DescribeAnalysis',
                    'quicksight:DescribeAnalysisPermissions',
                    'quicksight:QueryAnalysis',
                    'quicksight:UpdateAnalysis',
                    'quicksight:UpdateAnalysisPermissions',
                    'quicksight:DeleteAnalysis',
                    'quicksight:RestoreAnalysis'
                ]
            }
        ]
    )

# S3
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

bucket_name = "fantasy-analyzer-ralph"

create_s3Bucket(bucket_name) #Left blank to create in us-east-1
allow_public_access(bucket_name= bucket_name)
configure_bucket_policy(bucket_name, "public-read.json")

upload_file(bucket_name, "FantasyFootball-PPR-stats.csv", "FantasyFootball-PPR-stats.csv") 
upload_file(bucket_name, "manifest.json", "manifest.json")

# QuickSight
Account_ID = 'Accountid'

#For datasource
dataSource_ID = str(uuid.uuid4())

#For dataset
dataSet_ID = str(uuid.uuid4())
physicalSetMap_ID = str(uuid.uuid4())
logicalSetMap_ID = str(uuid.uuid4())

#For analysis
analysis_ID = str(uuid.uuid4())
sheet_ID = str(uuid.uuid4())

qs_client = boto3.client('quicksight')

sourceid = s3_to_qsdatasource(account_id= Account_ID, datasource_id= dataSource_ID, bucket_name= bucket_name, datasource_name= "fantasy-ds")
setid, set_name = qsdatasource_to_qsdataset(account_id= Account_ID, dataset_id= dataSet_ID, dataset_name= "fantasy-ds", pmap_id= physicalSetMap_ID, lmap_id = logicalSetMap_ID, datasource_id= sourceid)
qsdataset_to_analysis(account_id= Account_ID, analysis_id= analysis_ID, analysis_name= "fantasy-analysis", sheet_id= sheet_ID, sheet_name= "2022-Fantasy-Stats", dataset_name= set_name, dataset_id = setid)

