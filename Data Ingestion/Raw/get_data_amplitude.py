
import sys
import pyspark.sql.functions as func
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
import json
import boto3
import ast
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import gc
import sys
from pyspark.conf import SparkConf
import pandas as pd

print('Lectura de parámetros')

# ----------------------------------------------------------------------------------
print('NOW:', datetime.now())

args = getResolvedOptions(sys.argv,
                          ['bucket_amplitude_data', 
                           'today', 
                           'kms_key_arn', 
                           'recommendations_bucket'])

bucket_amplitude_data = args['bucket_amplitude_data']
recommendations_bucket = args['recommendations_bucket']
kms_key_id = args['kms_key_arn']
today = args['today']


#--------------------------------------------------------------------------------------------------------------

print('Spark Configuración')

spark_conf = SparkConf().setAll([
  ("spark.hadoop.fs.s3.enableServerSideEncryption", "true"),
  ("spark.hadoop.fs.s3.serverSideEncryption.kms.keyId", kms_key_id)
])

sc = SparkContext(conf=spark_conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()



print('Crear objetos S3-ssm')
# ----------------------------------------------------------------------------------
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
ssm = boto3.client('ssm')

#--------------------------------------------------------------------------------------------------------------
print('Parámetros:')
path_key_amplitude = 'ar/amplitude/tb_ar_amplitude_events_stage/'

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
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(buckets_)
    files_in_bucket = list(bucket.objects.all())
    files_objets = [f"s3://{buckets_}/" + i.key for i in files_in_bucket if
                        (i.key.find(keys_) >= 0) and (i.key.find('.parquet') >= 0)]
    df_bucket_files = pd.DataFrame({
            'key': [i[:(i.find('dt=') + 14)] for i in files_objets],
            'path': files_objets,
            'date': pd.to_datetime([i[(i.find('dt=') + 3):(i.find('dt=') + 13)] for i in files_objets])
        })
    files=list(df_bucket_files.loc[df_bucket_files['date'].between(str(first_day),str(last_day)),'path'].values)
    return files



map_events = {
    "cuoti_selecciona_elegircuotas" : "cuotificaciones",
    "cuoti_sigue_seleccion_consumos" : "cuotificaciones",
    "prestamos_selecciona_simular_prestamo": "prestamos",
    "prestamos_espera": "prestamos",
    "general_ingresa_promociones": "promociones",
    "recargas_click_empezar": "recargas",
    "recargas_click_repetir": "recargas",
    "transferencia_selecciona_tieneuala": "transferencia_c2c",
    "transferencia_selecciona_notieneuala": "transferencia_cvu",
    "general_ingresa_cobros": "cobros",
    "cobros_acepta_tyc" : "cobros",
    "cobros_elige_link": "cobros",
    "cobros_elige_mpos": "cobros",
    "pagos_empezar": "pago_servicios",
    "click_inversiones":"inversiones"
}

eventos_recommendations = list(map_events.keys())
#-----------------------------------------------------------------------------------------------------------------
first_day,last_day = first_and_last(today)
print('Primer dia',first_day)
print('Ultimo dia',last_day)

files_objets_amplitude = list_objects_function(bucket_amplitude_data, first_day, last_day ,path_key_amplitude)

print(f'Hay {len(files_objets_amplitude)} archivos de survival en la carpeta')
#df_amplitude = spark.read.parquet(*files_objets_amplitude).select(['user_id',"os_name","event_type","event_time"])
df_amplitude = spark.read.parquet(*files_objets_amplitude)
print('ANTES DE LIMPIEZA')
print(df.dtypes)
print(df.count(),len(df.columns))
df_amplitude=df_amplitude.select(['user_id',"os_name","event_type","event_time"])
df_amplitude=df_amplitude.filter(df_amplitude.event_type.isin(eventos_recommendations))

df_amplitude = df_amplitude.withColumn('year_month', F.date_format(df_amplitude.event_time,'YYYY-MM'))

df_amplitude = df_amplitude.drop("event_time")

df_amplitude = df_amplitude.na.replace(map_events,1,"event_type")

df_amplitude = (df_amplitude    
      .groupBy(['user_id', 'event_type', 'year_month'])
      .agg(F.count('event_type').alias('cant'),
           F.max('os_name').alias('os_name'))
      .groupBy(['user_id','year_month','os_name'])
      .pivot("event_type")
      .agg(F.sum('cant'))
      .na.fill(0)
      )

print('DESPUES DE LIMPIEZA')
print(df.dtypes)
print(df.count(),len(df.columns))

#df_amplitude.write\
#     .format('parquet')\
#     .save(f's3://{recommendations_bucket}/data/raw/amplitude/dt={str(first_day)}', mode='overwrite')

#print('Ubicación files', f's3://{recommendations_bucket}/data/raw/amplitude/dt={str(first_day)}')

#DELETE $FOLDER$

#def retrieve_files(path, file_type, list_dates):
#    bucket=path.split('/')[2]
#    prefix='/'.join(path.split('/')[3:])
#    list_objects=list(s3.Bucket(bucket).objects.all())
#    list_objects=[f's3://{bucket}/{i.key}' for i in list_objects if ((i.key.find(prefix)>=0) & any(x in i.key.lower() for x in list_dates) & (i.key.find(file_type)>=0))]
#    return list_objects
#
#
#delete_files = retrieve_files(path=f's3://{recommendations_bucket}/data/', file_type='$folder$', list_dates=['$folder$'])
#print('Files to delete', delete_files)
#files_keys=[]
#for i in range(0,len(delete_files)):
#    files_keys=files_keys+[{'Key':('/').join(delete_files[i].split('/')[3:])}]
#if len(files_keys)>0:
#    s3_client.delete_objects(Bucket=recommendations_bucket,
#                             Delete={'Objects':files_keys})
#del delete_files
#gc.collect()
#
#print(df_amplitude.show())
#print((df_amplitude.count(), len(df_amplitude.columns)))
