

import sys
import pyspark.sql.functions as func
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job
import pyspark.sql.functions as F
import json
import boto3
import ast
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import gc
from pyspark.conf import SparkConf
import pandas as pd
import os
from io import BytesIO
import awswrangler as wr

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=com.amazonaws:aws-java-sdk-bundle:1.11.271,org.apache.hadoop:hadoop-aws:3.1.2 pyspark-shell"


print('Lectura de parámetros')

# ----------------------------------------------------------------------------------
print('NOW:', datetime.now())

args = getResolvedOptions(sys.argv,
                          ['bucket_transactions_data', 
                           'today', 
                           'kms_key_arn', 
                           'recommendations_bucket'])

bucket_transactions_data = args['bucket_transactions_data']
recommendations_bucket = args['recommendations_bucket']
kms_key_id = args['kms_key_arn']
today = args['today']

#--------------------------------------------------------------------------------------------------------------


#https://stackoverflow.com/questions/52932459/accessing-s3-bucket-from-local-pyspark-using-assume-role


print('Crear objetos S3-ssm')
# ----------------------------------------------------------------------------------
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
ssm = boto3.client('ssm')

#--------------------------------------------------------------------------------------------------------------
print('Parámetros:')
path_key_transactions = 'ar/tb_ar_core_transactions/'
path_key_authorizations = 'ar/tb_ar_core_authorizations/'
path_key_cotizaciones = 's3://test-datascience-adquirencia-fraude/data/auxiliar/df_cotizacion.parquet'
#path_key_params = 's3a://uala-arg-datalake-stage-prod/ar/configs/tb_ar_configs_params/be46f255d9c44e59a70edbbf0b815874.snappy.parquet'
## FECHAS INTERVALO
#print('1. CALCULO DE FECHAS')
##Today llevado al primero del mes menos 1 día
#today = datetime.strptime(today, '%Y-%m-%d').date().replace(day=1)
#last_day=(today-pd.offsets.DateOffset(days=1)).date()
##
#first_day=(last_day-pd.offsets.DateOffset(days=365)).date()
#
#print('2. Intevalo de fechas analizada: ',first_day,'y',last_day)

def first_and_last(today):
    fecha=datetime.strptime(today, '%Y-%m-%d').date()
    first_day=fecha.replace(day=1)
    next_month = fecha.replace(day=28) + timedelta(days=4)
    last_day_of_month = next_month - timedelta(days=next_month.day)
    return first_day,last_day_of_month

print('Declaración de funciones')
def list_objects_function(buckets_, first_day, last_day, keys_, retrieve_last=False):
    
    sts = boto3.client('sts')
    response = sts.assume_role(
        RoleArn='arn:aws:iam::514405401387:role/aws-rol-ml-read-stage-prod', #es el rol que existe en produccion por el cual "nos hacemos pasar" para acceder a los buckets de s3
        RoleSessionName='sesion-dsr-recomendaciones', # nombre que le damos a la sesión
        DurationSeconds=3600 # es el tiempo que dura la sesion por default si no especificamos este parámetro.
    )

    s3 = boto3.client(
        's3',
        aws_access_key_id=response['Credentials']['AccessKeyId'],
        aws_secret_access_key=response['Credentials']['SecretAccessKey'],
        aws_session_token=response['Credentials']['SessionToken']
    )


    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=buckets_, Prefix=keys_)
    files_in_bucket=[]
    for page in pages:
        files_page=[key['Key'] for key in page['Contents']]
        files_in_bucket+=files_page
        files_objets = [f"s3a://{buckets_}/" + i for i in files_in_bucket if
                            (keys_ in i)  and (i.find('.parquet') >= 0)]
        df_bucket_files = pd.DataFrame({
                'key': [i[:(i.find('dt=') + 14)] for i in files_objets],
                'path': files_objets,
                'date': pd.to_datetime([i[(i.find('dt=') + 3):(i.find('dt=') + 13)] for i in files_objets])
            })
        files=list(df_bucket_files.loc[df_bucket_files['date'].between(str(first_day),str(last_day)),'path'].values)
    return files

#-----------------------------------------------------------------------------------------------------------------

########## INICIO CONFIG SPARK ###############
sts = boto3.client('sts')
response = sts.assume_role(
    RoleArn='arn:aws:iam::514405401387:role/aws-rol-ml-read-stage-prod', #es el rol que existe en produccion por el cual "nos hacemos pasar" para acceder a los buckets de s3
    RoleSessionName='sesion-dsr-spark', # nombre que le damos a la sesión
    DurationSeconds=3600 # es el tiempo que dura la sesion por default si no especificamos este parámetro.
)
print('Spark Configuración')
spark_conf = SparkConf().setAll([
  ("spark.hadoop.fs.s3.enableServerSideEncryption", "true"),
  ("spark.hadoop.fs.s3.serverSideEncryption.kms.keyId", kms_key_id)
])
sc = SparkContext(conf=spark_conf) 
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", response["Credentials"]["AccessKeyId"])
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", response["Credentials"]["SecretAccessKey"])
spark._jsc.hadoopConfiguration().set("fs.s3a.session.token",  response["Credentials"]["SessionToken"])
print(f"Hadoop version = {sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")
########## FIN CONFIG SPARK ###############

#-----------------------------------------------------------------------------------------------------------------

first_day,last_day = first_and_last(today)
print('Primer dia',first_day)
print('Ultimo dia',last_day)

files_objets_transactions= list_objects_function(bucket_transactions_data, first_day, last_day ,path_key_transactions)
print(f'Hay {len(files_objets_transactions)} archivos de transactions en la carpeta')

files_objets_authorizations= list_objects_function(bucket_transactions_data, first_day, last_day ,path_key_authorizations)
print(f'Hay {len(files_objets_authorizations)} archivos de transactions en la carpeta')

df_transactions = spark.read.parquet(*files_objets_transactions).dropDuplicates(['transaction_id']).select(["dt","transaction_id","account_from","amount","currency_code","transaction_type","status","authorization_id"])

df_authorizations = spark.read.parquet(*files_objets_authorizations).dropDuplicates(['authorization_id']).select(['authorization_id','metadata'])

s3=boto3.resource('s3')
df_param_mcc=pd.read_csv('s3://test-datascience-recommendations/data/param/param_mcc_category.csv')
df_param_mcc.mcc=df_param_mcc.mcc.astype(str)
df_param_mcc.rename(columns={'mcc':'mcc_param'},inplace=True)

df_transactions = df_transactions.filter(F.col("status").isin(['AUTHORIZED']))

df_cotizaciones=spark.createDataFrame(wr.s3.read_parquet(path_key_cotizaciones))

df_param_mcc=spark.createDataFrame(df_param_mcc)

df_transactions = df_transactions.join(df_cotizaciones, df_transactions['dt']==df_cotizaciones['fecha'], 'inner')

df_transactions = df_transactions.withColumn('amount_usd', F.col("amount")/F.col("venta_uala"))

df_transactions=df_transactions.join(df_authorizations, df_transactions["authorization_id"] == df_authorizations["authorization_id"], "left")

df_transactions = df_transactions.select("dt","account_from","amount_usd","transaction_type",F.get_json_object(df_transactions.metadata,'$.mcc').alias('mcc'))

df_transactions = df_transactions.withColumn('year_month', F.date_format(df_transactions.dt,'YYYY-MM'))

df_transactions = df_transactions.drop("dt")

df_transactions=df_transactions.join(df_param_mcc, df_transactions["mcc"] == df_param_mcc["mcc_param"], "left")#.na.fill('otros')

df_transactions.fillna('otros',subset=['category'])


##### ARMAMOS COMPRAS POR CATEGORIA USANDO MCC #######
df_transactions = df_transactions.withColumn('new_transaction_type',F.concat(F.when(F.col('transaction_type').isin(['AUTOMATIC_DEBIT','CONSUMPTION_POS']), F.lit('PURCHASE_')).otherwise(F.col('transaction_type')),F.when(F.col('transaction_type').isin(['AUTOMATIC_DEBIT','CONSUMPTION_POS']), F.upper(F.col('category'))).otherwise(F.lit(''))))
##### ARMAMOS COMPRAS POR CATEGORIA USANDO MCC #######

df_transactions = df_transactions.select("year_month","account_from","amount_usd","new_transaction_type")




df_transactions_nu = (df_transactions    
      .groupBy(['year_month', 'new_transaction_type','account_from'])
      .agg(F.count('new_transaction_type').alias('nu'))
      .groupBy(['account_from','year_month'])
      .pivot("new_transaction_type")
      .agg(F.sum('nu'))
      .na.fill(0)
      )

oldColumns = df_transactions_nu.schema.names
nonNuValues=["account_from","year_month"]
oldColumns=["NU_" + x for x in oldColumns if not str(x) in ["year_month","account_from","amount_usd"]]
newColumns=nonNuValues+oldColumns

df_transactions_nu=df_transactions_nu.toDF(*newColumns)



df_transactions_vl = (df_transactions    
      .groupBy(['year_month', 'new_transaction_type','account_from'])
      .agg(F.sum('amount_usd').alias('vl'))
      .groupBy(['account_from','year_month'])
      .pivot("new_transaction_type")
      .agg(F.sum('vl'))
      .na.fill(0)
      )

oldColumns = df_transactions_vl.schema.names
oldColumns=["VL_" + x for x in oldColumns]
df_transactions_vl=df_transactions_vl.toDF(*oldColumns)
join_condition = [df_transactions_nu["account_from"] == df_transactions_vl["VL_account_from"], df_transactions_nu["year_month"] == df_transactions_vl["VL_year_month"]]
df_transactions=df_transactions_nu.join(df_transactions_vl, join_condition, "inner").drop('VL_account_from','VL_year_month','VL_null','NU_null')




print(spark.sparkContext.getConf().getAll())

#### NUEVA INSTANCIA boto3 para usar buckets en stage #####
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
ssm = boto3.client('ssm')

df_pandas=df_transactions.toPandas()
df_pandas['dt'] = first_day
wr.s3.to_parquet(df_pandas,
                        path='s3://{}/data/raw/transactions/'.format(recommendations_bucket),
                        dataset=True,
                        partition_cols=['dt'],
                        mode="append",
                        concurrent_partitioning=True,
                        index=False)

print('Ubicación files', f's3://{recommendations_bucket}/data/raw/transactions/dt={str(first_day)}')

gc.collect()
print('TRANSACTIONS')
#print(df_transactions.show())
print((df_transactions.count(), len(df_transactions.columns)))
print(df_transactions.dtypes)
print(df_transactions.show())
