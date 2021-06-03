
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
                          ['today', 
                           'kms_key_arn', 
                           'recommendations_bucket'])

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
path_key_survival_stg = 'data/raw/transactions/'
path_key_amplitude = 'data/raw/amplitude/'
path_key_cards = 'data/raw/cards/'

#s3://uala-arg-datalake-aiml-survival-dev/data/monthly_stage/
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

def list_objects_cards(buckets_, keys_):
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(buckets_)
    files_in_bucket = list(bucket.objects.all())
    files_objets = [f"s3://{buckets_}/" + i.key for i in files_in_bucket if
                        (i.key.find(keys_) >= 0) and (i.key.find('.parquet') >= 0)]
    df_bucket_files = pd.DataFrame({
            'key': [i for i in files_objets],
            'path': files_objets
        })
    files=list(df_bucket_files.loc[:,'path'].values)
    return files
#-----------------------------------------------------------------------------------------------------------------
first_day,last_day = first_and_last(today)
print('Primer dia',first_day)
print('Ultimo dia',last_day)

#Transacciones obtenidas de bucket de survival
files_objects_survival = list_objects_function(recommendations_bucket, first_day, last_day ,path_key_survival_stg)


print(f'Hay {len(files_objects_survival)} archivos de survival en la carpeta')
df_survival = spark.read.parquet(*files_objects_survival).select(['accountgp',  
                            'vl_cashin_prestamos_sum', 'vl_cashin_adquirencia_sum',
                            'nu_cashin_prestamos_qty',  'nu_cashin_adquirencia_qty',  
                            'nu_tcc_r_aprob', 'nu_tcc_t_aprob', 'nu_tcc_z_aprob', 'vl_tcc_r_aprob', 'vl_tcc_t_aprob', 'vl_tcc_z_aprob', 
                            'nu_mode_digital_qty_0_aprob', 'nu_mode_digital_qty_1_aprob', 'nu_automatic_debit_aprob', 'nu_cash_out_cvu_aprob', 
                            'nu_consumption_pos_aprob', 'nu_investments_withdraw_aprob', 'nu_telerecargas_carga_aprob', 'nu_user_to_user_aprob', 
                            'nu_withdraw_atm_aprob', 'vl_automatic_debit_aprob', 'vl_cash_out_cvu_aprob', 'vl_consumption_pos_aprob', 
                            'vl_investments_withdraw_aprob', 'vl_telerecargas_carga_aprob', 'vl_user_to_user_aprob', 'vl_withdraw_atm_aprob', 
                            'nu_compras_aprob', 'nu_entretenimiento_aprob', 'nu_servicios_débitos_automaticos_aprob', 'nu_supermercados_alimentos_aprob',
                            'nu_transferencias_retiros_aprob', 'vl_compras_aprob', 'vl_entretenimiento_aprob', 'vl_servicios_débitos_automaticos_aprob', 
                            'vl_supermercados_alimentos_aprob', 'vl_transferencias_retiros_aprob', 'nu_tcc_r_rech', 'nu_tcc_t_rech', 'nu_tcc_z_rech', 
                            'vl_tcc_r_rech', 'vl_tcc_t_rech', 'vl_tcc_z_rech', 'nu_mode_digital_qty_0_rech', 'nu_mode_digital_qty_1_rech', 'nu_automatic_debit_rech', 
                            'nu_cash_out_cvu_rech', 'nu_consumption_pos_rech', 'nu_investments_withdraw_rech', 'nu_telerecargas_carga_rech', 'nu_user_to_user_rech', 
                            'nu_withdraw_atm_rech', 'vl_automatic_debit_rech', 'vl_cash_out_cvu_rech', 'vl_consumption_pos_rech', 'vl_investments_withdraw_rech', 
                            'vl_telerecargas_carga_rech', 'vl_user_to_user_rech', 'vl_withdraw_atm_rech', 'nu_compras_rech', 'nu_entretenimiento_rech', 
                            'nu_servicios_débitos_automaticos_rech', 'nu_supermercados_alimentos_rech', 'nu_transferencias_retiros_rech', 
                            'vl_compras_rech', 'vl_entretenimiento_rech', 'vl_servicios_débitos_automaticos_rech', 'vl_supermercados_alimentos_rech', 'vl_transferencias_retiros_rech'])


df_survival = df_survival.withColumnRenamed("nu_investments_withdraw_aprob","nu_investments_deposit_aprob")\
                            .withColumnRenamed("vl_investments_withdraw_aprob", "vl_investments_deposit_aprob")\
                            .withColumnRenamed("nu_investments_withdraw_rech","nu_investments_deposit_rech")\
                            .withColumnRenamed("vl_investments_withdraw_rech","vl_investments_deposit_rech")


#Datos Cards
files_objects_cards = list_objects_cards(recommendations_bucket,path_key_cards)
print(files_objects_cards)
df_cards=spark.read.parquet(*files_objects_cards).select(['account_id','external_id']).dropDuplicates()


#Datos amplitud
files_objects_amplitude = list_objects_function(recommendations_bucket, first_day, last_day ,path_key_amplitude)
df_amplitude = spark.read.parquet(*files_objects_amplitude)


print('Size df_cards',df_cards.count())
print('Fila Amplitude',df_amplitude.count())
df_amplitude=df_amplitude.join(df_cards, df_amplitude["user_id"] == df_cards["account_id"], "inner")
print('Filas Amplitude despues de inner join con cards',df_amplitude.count())
df_amplitude=df_amplitude.join(df_survival, df_amplitude["external_id"] == df_survival["accountgp"], "left")
print('Filas Amplitude despues de left join con transactions',df_amplitude.count())

#LIMPIEZA INICIAL DE LOS DATOS
columns_to_drop = ['accountgp', 'user_id']

df_amplitude=df_amplitude.drop(*columns_to_drop).na.fill(0)


#Guardamos info procesada en bucket de STAGE
df_amplitude.write\
     .format('parquet')\
     .save(f's3://{recommendations_bucket}/data/stage/dt={str(first_day)}', mode='overwrite')

#DELETE $FOLDER$

def retrieve_files(path, file_type, list_dates):
    bucket=path.split('/')[2]
    prefix='/'.join(path.split('/')[3:])
    list_objects=list(s3.Bucket(bucket).objects.all())
    list_objects=[f's3://{bucket}/{i.key}' for i in list_objects if ((i.key.find(prefix)>=0) & any(x in i.key.lower() for x in list_dates) & (i.key.find(file_type)>=0))]
    return list_objects


delete_files = retrieve_files(path=f's3://{recommendations_bucket}/data/', file_type='$folder$', list_dates=['$folder$'])
print('Files to delete', delete_files)
files_keys=[]
for i in range(0,len(delete_files)):
    files_keys=files_keys+[{'Key':('/').join(delete_files[i].split('/')[3:])}]
if len(files_keys)>0:
    s3_client.delete_objects(Bucket=recommendations_bucket,
                             Delete={'Objects':files_keys})
del delete_files
gc.collect()

#print(df_cards.show(30))
#print((df_cards.count(), len(df_cards.columns)))
#print(df_cards.columns)
#print(df_amplitude.show())
#print(df_amplitude.columns)
