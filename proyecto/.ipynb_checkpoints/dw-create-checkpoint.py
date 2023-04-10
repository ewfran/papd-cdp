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


# ---
# In[ ]:




