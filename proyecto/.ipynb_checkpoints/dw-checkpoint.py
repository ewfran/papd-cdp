#!/usr/bin/env python
# coding: utf-8

# # INIT 

# #### >\_ LIBRERIAS Y CONFIG 

# In[82]:


import pandas as pd
import numpy as np
import boto3
import psycopg2
import configparser
import io


# In[124]:


config = configparser.ConfigParser()
config.read('proy.cfg')


# In[19]:


#RDS_HOST = 'cdp-proy-db.cmvrcaidz2tn.us-east-1.rds.amazonaws.com'
RDS_HOST=config.get('RDS', 'RDS_HOST')
#RDS_HOST
print(RDS_HOST)


# In[14]:


# DRIVER DE POSTGRES
postgres_driver = f"""postgresql://{config.get('RDS', 'DB_USER')}:{config.get('RDS', 'DB_PASSWORD')}@{RDS_HOST}:{config.get('RDS', 'DB_PORT')}/{config.get('RDS', 'DB_NAME')}"""  


# ---

# # SCOPE
# ### fuentes de inforamcion

# ### Fuente 1: AWS / RDS
# #### Descripcion de la BBDD OLTP
# + **motor:** PostgreSQL
# La base de datos sirve a un sistema de Call Center que brinda Asistencias para asegurados. Se tienen registros de las expedientes solicitando asistencia de distintas areas durante el año 2015. Las tablas se describen a continuacion:
# + **compania:** contiene los distintos clientes que contratan el servicio de Asistencias (no asegurados)  
# + **costo:**   tabla de hechos que contiene los servicios prestados, el proveedor y los costos generados
# + **departamento:**  catalogo de departamentos de Guatemala
# + **encargado:**   catalogo de agentes de call center que coordinan los servicios de asistencia
# + **expediente:**  tabla de hechos que contiene fecha, datos de asegurado, datos del servicio, y medidas de los servicios prestados.
# + **municipio:**  catalogo de municipios de Guatemala
# + **pais:**   catalogo de paises
# + **prestacion:**  catalogo de servicios disponibles para brindar asistencia
# 
# ### Fuente 2: AWS / S3
# #### Descripcion de los ficheros
# + **motor:** Archivos CSV
# + **container:** S3
# Los ficheros son estructuras complementarias del la BBDD OLTP del Sistema de Asistencias, son ficheros que contienen estructura y datos que provienen de un segundo sistema OLTP, y se adicionan al DataWarehouse. Las tablas se describen a continuacion:
# + **area:** categorias de los servicios prestados en la tabla: expediente  
# + **proveedor:**  catalogo de proveedores concertados de los que se pueden disponer en los expedientes de asistencia.
# 

# ---

# # EXPLORACIÓN DE DATOS

# ### >> COMPANIA

# In[200]:


sql_query = 'SELECT * FROM compania;'
df_compania = pd.read_sql(sql_query, postgres_driver)
df_compania.head()


# In[209]:


print(df_compania.shape)
print(df_compania['cod_com'].describe())
print(df_compania.columns)


# ### >_Preparar Dimension: dim_compania

# In[170]:


df_compania.drop_duplicates(subset=['cod_com'], inplace=True)
dim_compania = df_compania.drop(['cod_pai', 'tip_com', 'est_com', 'clasifica'], axis=1)
#dim_compania = df_compania.reset_index().rename({'index' : 'sk_com'}, axis='columns')
#df_compania.head()
#.rename({'index' : 'sk_com'}, axis='columns')
#dim_compania = df_compania
dim_compania.head()


# ### >> PAIS

# In[35]:


sql_query = 'SELECT * FROM pais;'
df_pais = pd.read_sql(sql_query, postgres_driver)
df_pais.head()


# In[210]:


print(df_pais.shape)
print(df_pais.describe())
print(df_pais.columns)


# ### >_Preparar Dimension: dim_pais

# In[36]:


dim_pais = df_pais


# ### >> DEPARTAMENTO

# In[33]:


sql_query = 'SELECT * FROM departamento;'
df_departamento = pd.read_sql(sql_query, postgres_driver)
df_departamento.head()


# In[211]:


print(df_departamento.shape)
print(df_departamento.describe())
print(df_departamento.columns)


# ### >_Preparar Dimension: dim_departamento

# In[38]:


df_departamento = df_departamento.merge(dim_pais)
dim_departamento = df_departamento.drop(['cod_pai'], axis=1)
dim_departamento.head()


# ### >> MUNICIPIO

# In[42]:


sql_query = 'SELECT * FROM municipio;'
df_municipio = pd.read_sql(sql_query, postgres_driver)
df_municipio.head()


# In[212]:


print(df_municipio.shape)
print(df_municipio.describe())
print(df_municipio.columns)


# ### >_Preparar Dimension: dim_municipio

# In[43]:


dim_municipio = df_municipio.reset_index().rename({'index' : 'sk_mun'}, axis='columns')
dim_municipio.head()


# ### >> ENCARGADO

# In[45]:


sql_query = 'SELECT * FROM encargado;'
df_encargado = pd.read_sql(sql_query, postgres_driver)
df_encargado.columns


# In[213]:


print(df_encargado.shape)
print(df_encargado.describe())
print(df_encargado.columns)


# ### >_Preparar Dimension: dim_encargado

# In[48]:


dim_encargado = df_encargado.drop(['cla_enc', 'flag', 'pathfoto',
       'emp_enc', 'foto', 'msgpriv', 'online', 'area', 'cargo', 'conexion',
       'dpi', 'direccion', 'telefono', 'movil', 'email', 'profesion', 'numa',
       'historial', 'metas', 'hobbie', 'comenta'], axis=1)
dim_encargado.head()
#dim_encargado[dim_encargado.duplicated('cod_enc')]


# In[196]:


dim_encargado.shape


# ### >> PRESTACION

# In[70]:


sql_query = 'SELECT * FROM prestacion;'
df_prestacion = pd.read_sql(sql_query, postgres_driver)
df_prestacion.columns


# In[214]:


print(df_prestacion.shape)
print(df_prestacion.describe())
print(df_prestacion.columns)


# ### >_Preparar Dimension: dim_prestacion

# **>> ELIMINAR DUPLICADOS EN LA DIMENSION**

# In[71]:


df_prestacion[df_prestacion.duplicated('cod_pre')]


# In[72]:


df_prestacion.drop_duplicates(subset=['cod_pre'], inplace=True)
dim_prestacion = df_prestacion.drop(['flag_prov', 'flag_srv2'], axis=1)
#dim_prestacion.head()
dim_prestacion[dim_prestacion.duplicated('cod_pre')]


# In[73]:


dim_prestacion.head()


# ### FUENTE: S3 - OBTENERMOS TABLAS DESDE  S3

# In[74]:


s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


# In[75]:


for bucket in s3.buckets.all():
    S3_BUCKET_NAME = bucket.name
    print(bucket.name)


# In[78]:


S3_BUCKET_NAME = config.get('S3', 'BUCKET_NAME')


# ### >> PROVEEDOR

# In[174]:


try:
    file = s3.Bucket(S3_BUCKET_NAME).Object('dim_proveedor.csv').get()
#    print(file)
    df_proveedor= pd.read_csv(file['Body'])
except Exception as ex:
    print("No es un archivo.")
    print(ex)

#df_proveedor.head()
df_proveedor.columns


# In[215]:


print(df_proveedor.shape)
print(df_proveedor.describe())
print(df_proveedor.columns)


# ### >_ Preparar Dimension: dim_proveedor
# **VERIFICAMOS SI HAY DUPLICADOS Y LOS ELIMINAMOS**

# In[175]:


len(df_proveedor[df_proveedor.duplicated('cod_pro')].index)


# In[176]:


df_proveedor.drop_duplicates(subset=['cod_pro'], inplace=True)
df_proveedor = df_proveedor.dropna(subset=["cod_pro"])
dim_proveedor = df_proveedor.drop(['cod_dep', 'cti_pro', 'te1_pro', 'te2_pro',
       'te3_pro', 'te4_pro', 'te5_pro', 'te6_pro', 'te7_pro', 'te8_pro',
       'est_pro', 'ind_imp', 'ind_ret', 'dir_pro', 'ser_pro', 'cpr_sap',
       'obs_pro', 'flag_prov', 'codigoe'], axis=1)
#dim_prestacion.head()
dim_proveedor[dim_proveedor.duplicated('cod_pro')]


# In[177]:


dim_proveedor.head()


# ### >> AREA

# In[92]:


try:
    file = s3.Bucket(S3_BUCKET_NAME).Object('dim_tipoprov.csv').get()
#    print(file)
    df_area= pd.read_csv(file['Body'])
except Exception as ex:
    print("No es un archivo.")
    print(ex)
df_area.head()


# In[216]:


print(df_area.shape)
print(df_area.describe())
print(df_area.columns)


# ### >_Preparar Dimension: dim_area

# In[128]:


dim_area = df_area.drop(['row_id'], axis=1)
dim_area.head()


# ### >> TABLA DE HECHOS

# In[132]:


sql_query = 'SELECT * FROM expediente;'
df_fact = pd.read_sql(sql_query, postgres_driver)
df_fact.columns
#df_expediente.head()


# In[219]:


print(df_fact.shape)
#print(df_fact.describe())
print(df_fact.columns)


# ### >_Preparar Dimension: fact_expediente

# In[133]:


df_fact['id_fecha'] = df_fact['fec_ser'].astype(str)
#factTable_transacciones = df_factTable.drop(['year', 'month', 'day', 'hour', 'minute', 'fecha_hora'], axis=1)
df_fact.drop(['locali', 'produc', 'cod_enc',
       'des_pro', 'let_cat', 'let_tip', 'fec_ser', 'fec_ini', 'fecha_crea'], axis=1, inplace=True)
#dim_prestacion.head()
df_fact.columns


# ### >_ agregamos sk tiempo

# In[134]:


df_fact.head()


# ### >> COSTO

# In[130]:


sql_query = 'SELECT * FROM costo;'
df_costo = pd.read_sql(sql_query, postgres_driver)
df_costo.columns
#df_expediente.head()


# In[220]:


print(df_costo.shape)
#print(df_costo.describe())
print(df_costo.columns)


# ### >_Preparar costo

# In[131]:


df_costo.drop(['num_lin', 'cod_dep', 'cti_pro', 'cod_are', 'let_cat', 'let_tip',  
        'fechor', 'tie_lle', 'exced', 'klle', 'kvac', 'kter', 'cod_eco', 'cod_rec', 'cod_ese',
       'formuprove', 'reclamo', 'id', 'calculo'], axis=1, inplace=True)
#dim_prestacion.head()
df_costo.head()


# In[135]:


#df_fact_dep = df_fact.merge(dim_departamento, how='left', left_on='cod_dep', right_on='nom_dep')
#df_fact_dep_mun = df_fact_dep.merge(dim_municipio, how='left', left_on='cod_mun', right_on='nom_mun')
df_fact_costo = df_fact.merge(df_costo, how='left', on='num_exp')
df_fact_costo.head()


# #### >> VERIFICAMOS SI HAY DUPLICADOS

# In[ ]:


len(df_fact_costo[df_fact_costo.duplicated('num_exp')].index)


# In[136]:


fact_expediente = df_fact_costo.reset_index().rename({'index' : 'sk_exp'}, axis='columns')


# In[137]:


fact_expediente.columns


# In[138]:


fact_expediente.head()


# ### >_Preparar Dimension de TIEMPO: dim_fecha

# In[112]:


sql_query = 'SELECT fec_ser FROM expediente;'
df_fecha = pd.read_sql(sql_query, postgres_driver)
df_fecha.head()


# In[113]:


df_fecha['year'] = pd.DatetimeIndex(df_fecha['fec_ser']).year
df_fecha['month'] = pd.DatetimeIndex(df_fecha['fec_ser']).month
df_fecha['quarter'] = pd.DatetimeIndex(df_fecha['fec_ser']).quarter
df_fecha['day'] = pd.DatetimeIndex(df_fecha['fec_ser']).day
df_fecha['week'] = pd.DatetimeIndex(df_fecha['fec_ser']).week
df_fecha['dayofweek'] = pd.DatetimeIndex(df_fecha['fec_ser']).dayofweek
df_fecha['is_weekend'] = df_fecha['dayofweek'].apply(lambda x: 1 if x > 5 else 0)
df_fecha.head()


# In[186]:


df_fecha['id_fecha'] = df_fecha['fec_ser'].astype(str)
df_fecha.rename({'fec_ser': 'fecha'}, axis=1, inplace=True)
dim_fecha = df_fecha.drop_duplicates()
dim_fecha.head()


# ---

# # MODELO DE DATOS

# ### Definir el modelo del DW
# #### Descripcion del modelo BBDD OLAP
# + **motor:** PostgreSQL
# + **host:** AWS / RDS
# La Base de Datos se diseña con el Modelo Dimensional tipo Estrella. Se define como Tabla de Hechos "dim_expediente" que contiene variables medibles y llaves de las dimensiones que describen cada observacion. Las tablas se describen a continuacion:
# + **fact_expediente:**  tabla de hechos que contiene la fecha, datos llaves descriptivas del servicio, costos del servicio, tipo de servicio y proveedor que atiende el servicio.
# + **dim_compania:** descripcion de distintos clientes que contratan el servicio de Asistencias (no asegurados).
# + **dim_departamento:**  descripcion de departamentos de Guatemala.
# + **dim_encargado:**   descripcion de los agentes de call center que coordinan los servicios.
# + **dim_municipio:**  descripcion de los municipios de Guatemala
# + **dim_pais:**   descripcion de paises
# + **dim_prestacion:**  descripcion de los servicios disponibles.
# + **dim_area:** descripcion de las categorias de los servicios en expedientes.  
# + **dim_proveedor:**  descripcion de los proveedores concertados en los expedientes de asistencia.
# + **dim_fecha:**  descripcion de cada fecha con distintos caracteristicas.
# 
# ![](lib/dwh-public.png)

# ---

# ---

# # PROCESAMIENTO

# In[118]:


aws_conn = boto3.client('rds', aws_access_key_id=config.get('IAM', 'ACCESS_KEY'),
                    aws_secret_access_key=config.get('IAM', 'SECRET_ACCESS_KEY'),
                    region_name='us-east-1')
print(aws_conn)


# In[139]:


rdsInstanceIds = []

response = aws_conn.describe_db_instances()
for resp in response['DBInstances']:
    rdsInstanceIds.append(resp['DBInstanceIdentifier'])
    db_instance_status = resp['DBInstanceStatus']

print(f"DBInstanceIds {rdsInstanceIds}")


# ### Crear la Instancia de BBDD para en DW en AWS / RDS
# 
# **- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -**  
# << ESTE CODIGO SE EJECUTA UNA SOLA VEZ >>

# In[140]:


rdsIdentifier = 'cdp-proy-dw' #nombre de la instancia


# In[143]:


try:
    response = aws_conn.create_db_instance(
            AllocatedStorage=10,
            DBName=config.get('DW', 'DB_NAME'),
            DBInstanceIdentifier=rdsIdentifier,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername=config.get('DW', 'DB_USER'),
            MasterUserPassword=config.get('DW', 'DB_PASSWORD'),
            Port=int(config.get('DW', 'DB_PORT')),
            VpcSecurityGroupIds=[config.get('VPC', 'SECURITY_GROUP')],
            PubliclyAccessible=True
        )
    print(response)
except aws_conn.exceptions.DBInstanceAlreadyExistsFault as ex:
    print("La Instancia de Base de Datos ya Existe.")


# << ESTE CODIGO SE EJECUTA UNA SOLA VEZ >>  
# ### - - - - - - - -  fin de creacion de Instancia AWS / RDS - - - - - - - - - - - - - - - - - - 

# ##### Antes de conectarnos recordemos esperar el tiempo suficiente para agregar la nueva instancia y agregar el puerto 3306 al grupo de seguridad.

# In[144]:


try:
     instances = aws_conn.describe_db_instances(DBInstanceIdentifier=rdsIdentifier)
     RDS_DW_HOST = instances.get('DBInstances')[0].get('Endpoint').get('Address')
     print(RDS_DW_HOST)
except Exception as ex:
     print("La instancia de base de datos no existe o aun no se ha terminado de crear.")
     print(ex)


# ### >>  CONEXION A LA BBDD - DW

# In[156]:


import sql_create_tables

try:
    db_conn = psycopg2.connect(
        database=config.get('DW', 'DB_NAME'), 
        user=config.get('DW', 'DB_USER'),
        password=config.get('DW', 'DB_PASSWORD'), 
        host=RDS_DW_HOST,
        port=config.get('DW', 'DB_PORT')
    )

    cursor = db_conn.cursor()
    cursor.execute(sql_create_tables.CREATE_DW)
    db_conn.commit()
    print("Base de Datos Creada Exitosamente")
except Exception as ex:
    print("ERROR: Error al crear la base de datos.")
    print(ex)


# ### >> POBLAMOS LAS DIMENSIONES DEL DW CON LOS DATAFRAMES PREPARADOS

# >_ funcion para insertar datos en AWS/RDS

# In[162]:


def df_to_dw(table_name, df_data):
     postgres_driver = f"""postgresql://{config.get('DW', 'DB_USER')}:{config.get('DW', 'DB_PASSWORD')}@{RDS_DW_HOST}:{config.get('DW', 'DB_PORT')}/{config.get('DW', 'DB_NAME')}"""    
     try:
          response = df_data.to_sql(table_name, postgres_driver, index=False, if_exists='append')
          print(f'Se han insertado {response} nuevos registros.' )
     except Exception as ex:
          print(ex)


# #### >> Insert data en Dimension: _-- dim_area --_

# In[163]:


df_to_dw('dim_area', dim_area)


# #### >> Insert data en Dimension: _-- dim_compania --_

# In[171]:


df_to_dw('dim_compania', dim_compania)


# #### >> Insert data en Dimension: _-- dim_encargado --_

# In[172]:


df_to_dw('dim_encargado', dim_encargado)


# #### >> Insert data en Dimension: _-- dim_proveedor --_

# In[178]:


df_to_dw('dim_proveedor', dim_proveedor)


# #### >> Insert data en Dimension: _-- dim_prestacion --_

# In[179]:


df_to_dw('dim_prestacion', dim_prestacion)


# #### >> Insert data en Dimension: _-- dim_pais --_

# In[180]:


df_to_dw('dim_pais', dim_pais)


# #### >> Insert data en Dimension: _-- dim_departamento --_

# In[181]:


df_to_dw('dim_departamento', dim_departamento)


# #### >> Insert data en Dimension: _-- dim_municipio --_

# In[182]:


df_to_dw('dim_municipio', dim_municipio)


# #### >> Insert data en Dimension: _-- dim_fecha --_

# In[188]:


df_to_dw('dim_fecha', dim_fecha)


# #### >> Insert data en Tabla de Hechos: _-- fact_expediente --_

# In[195]:


df_to_dw('fact_expediente', fact_expediente)


# ---

# ---

# # ANALITICA
# ### Planteamiento para el analisis de datos con el DW definido

# #### Teniendo en cuenta el modelo de Datawarehouse definido, se plantean las siguientes preguntas: 
# 
# + **¿Qué dia(s) de la semana y del mes es en donde se reporta la mayor siniestralidad?**   
# + Ya que se tienen los datos del vehiculo **¿Se puede predecir como será la siniestralidad de un asegurado al terminar el año, para asignar el costo de la siguiente renovación de la prima del seguro?**   
# + El gerente cada cierto periodo de tiempo recibe quejas de los clietnes porque es muy alto el tiempo de espera en call center, entonces decide contratar mas personal, pero despues debe recortar personal porque baja la demanda y se reducen los TMO. **¿Se pueden conocer las estacionalidades en comportamiento de la demanda de servicios. Para saber con anticipacion cuando necesitara personal extra y por cuanto tiempo?**   
# + **Se desean acomodar los horarios de Call Center para reducir los TMO altos que existen en algunas horas del día en ciertos dias, pero sin afectar los tiempos de los demas horarios. Y así poder saber si se puede mejorar el nivel de servicio optimizando con los recursos que ya se cuentan, o si es necesario contratar mas personal**
# + **El gerente financiero, para asignar los niveles de precio de las primas de seguro, y quiere saber si se puede predecir la siniestralidad que tendrá cierto vehiculo en el futuro. Desea saber si existe relacion alguna entre el riesgo de siniestralidad de una futura poliza con los datos del solicitante: marca, modelo y linea del vehiculo, edad y datos personales del solicitante; pide analizar si existe relación alguna y si es posible establecer precio de la prima basados en este analisis.**   
# 
# 
# 
# 

# ---

# In[ ]:




