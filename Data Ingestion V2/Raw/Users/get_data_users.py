

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

path_equivalencia_gp_uala='s3://test-datascience-adquirencia-fraude/data/auxiliar/account_id_gp.parquet'

print('Lectura de parámetros')

# ----------------------------------------------------------------------------------
print('NOW:', datetime.now())

args = getResolvedOptions(sys.argv,
                          ['bucket_users_data', 
                           'today', 
                           'kms_key_arn', 
                           'recommendations_bucket'])

bucket_users_data = args['bucket_users_data']
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
path_key_users = 'ar/tb_ar_gp_t1010/'
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

def list_objects(buckets_, keys_):
    
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
                'key': [i for i in files_objets],
                'path': files_objets
            })
        files=list(df_bucket_files.loc[:,'path'].values)
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


print('Lectura users')
files_objets_users= list_objects(bucket_users_data, path_key_users)
print(files_objets_users)
print(f'Hay {len(files_objets_users)} archivos de transactions en la carpeta')

df_1010 = spark.read.parquet(*files_objets_users).dropDuplicates(['numero_cuenta']).select(['numero_cuenta', 'fecha_alta', 'provincia', 'sexo', 'fecha_nacimiento'])
print((df_1010.count(), len(df_1010.columns)))
print(df_1010.dtypes)
print(df_1010.show())
# dtypes
df_1010 = (df_1010
          .withColumn('numero_cuenta', df_1010.numero_cuenta.cast('string'))
          .withColumn('fecha_alta', F.to_date(df_1010.fecha_alta,"yyyy-MM-dd"))
          .withColumn('fecha_nacimiento', F.to_date(df_1010.fecha_nacimiento,"yyyy-MM-dd"))
          .withColumn('provincia', df_1010.provincia.cast('int'))
         )

# Edad y antiguedad
df_1010 = (df_1010
          .withColumn('edad', F.floor(F.datediff(F.current_date(), F.col('fecha_nacimiento'))/365.25))
          .withColumn('antiguedad', F.datediff(F.current_date(), F.col('fecha_alta')))
         )
          
df_1010 = df_1010.drop('fecha_nacimiento','fecha_alta')
          
df_1010 = df_1010.withColumnRenamed('numero_cuenta','externalid')

# Provincia int a str
dict_param_province = {0:"Undefined",
                    1:"capital federal",
                    2:"gran buenos aires",
                    3:"buenos aires",
                    4:"catamarca",
                    5:"cordoba",
                    6:"corrientes",
                    7:"chaco",
                    8:"chubut",
                    9:"entre rios",
                    10:"formosa",
                    11:"jujuy",
                    12:"la pampa",
                    13:"la rioja",
                    14:"mendoza",
                    15:"misiones",
                    16:"neuquen",
                    17:"rio negro",
                    18:"salta",
                    19:"san juan",
                    20:"san luis",
                    21:"santa fe",
                    22:"santa cruz",
                    23:"santiago del estero",
                    24:"tierra del fuego",
                    25:"tucuman"}

df_1010 = df_1010.withColumn("provincia", df_1010["provincia"].cast(IntegerType()))
df_1010 = df_1010.withColumn("provincia", df_1010["provincia"].cast(StringType()))
dict_param_province = {str(k):v for k,v in zip(dict_param_province.keys(), dict_param_province.values())}
df_1010 = df_1010.na.replace(dict_param_province, 1, "provincia")
print(spark.sparkContext.getConf().getAll())

# JOINEAMOS CON CARDS PARA TENER LA EQUIVALENCIA ENTRE ACCOUNT DE GP Y ACCOUNT DE UALA
column_names=['account_uala','account_gp']
df_gp_uala=spark.createDataFrame(wr.s3.read_parquet(path_equivalencia_gp_uala)).toDF(*column_names)

df_1010=df_1010.join(df_gp_uala,df_1010['externalid']==df_gp_uala['account_gp'],"inner").drop('externalid','account_gp')


print(df_1010.show())
#### NUEVA INSTANCIA boto3 para usar buckets en stage #####
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
ssm = boto3.client('ssm')

df_pandas=df_1010.toPandas()

wr.s3.to_parquet(df_pandas,
                path='s3://{}/data/raw/users/'.format(recommendations_bucket),
                dataset=True,
                index=False)

print('Ubicación files', f's3://{recommendations_bucket}/data/raw/users/')

gc.collect()
print('USERS')
#print(df_1010.show())
print((df_1010.count(), len(df_1010.columns)))
print(df_1010.dtypes)
print(df_1010.show())
