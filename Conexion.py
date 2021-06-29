import os # librería para leer variables del s.o.
import psycopg2
from boto3.session import Session

def create_conn(**kwargs):
    '''
    Funcion para crear la conexion activada con la base de datos 
    Parametros:
        config['dbname'] : str. nombre de base de datos
        config['host'] : str. Host
        config['port'] : str. puerto 
        config['user'] : str. Nombre usuario
        config['password'] : str. Password de la base de datos
    Return: 
        con = obejct. new connection object            
    '''
    config = kwargs
    print('def conexion')
    try:
        conn=psycopg2.connect(dbname = config['dbname'],
                        host = config['host'],
                        port = config['port'],
                        user = config['user'],
                        password = config['password'])
        print('conexion')
    except Exception as err:
        print('error')
        print(err.code, err)
    
    return conn

def ConexionBD():
    '''
    Funcion para la conexion de la base de datos redshift 
    Parametros: null
    Return: 
        con: object. Conexion activa.            
    '''
    
    config = {"dbname":os.environ['AWS_RS_DB_NAME'],
            "host": os.environ['AWS_RS_DB_HOST'],
            "port": os.environ['AWS_RS_DB_PORT'], 
            "user": os.environ['AWS_RS_DB_USER'],
            "password":os.environ['AWS_RS_DB_PASS']}
    print('creacion de la conexion def')
    conn = create_conn(**config)
    return conn

def ConexionBucket(NameBucket):
    '''
    Funcion para retornar los recuross de un bucket 
    Parametros: 
        NameBucket: str. nombre del bucket
    Return: 
        bucket: object. A Bucket resource     
        s3: object. A Bucket resource     
    '''
    ACCESS_KEY = os.environ['ACCESS_KEY']
    SECRET_KEY = os.environ['SECRET_KEY']
    
    session = Session(aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)
    s3 = session.resource('s3')
    bucket = s3.Bucket(NameBucket)
    
    return bucket,s3

def GeneraQuery(FileName, Bucket):
    '''
    Funcion para generar la query de carga de achivos 
    Parametros: 
        FileName: str. nombre del achivo
        Bucket: str. nombre del bucket
    Return: 
        query: str. query formada           
    '''
    key = os.environ['ACCESS_KEY']
    secret = SECRET_KEY = os.environ['SECRET_KEY']
    
    query= """
    copy dev.public.medicamento (Age,Sex,BP,Cholesterol,Na_to_K,Drug)
    from 's3://{}/{}'
    access_key_id '{}'
    secret_access_key '{}'
    csv
    IGNOREHEADER 1 
    delimiter ','
    TRUNCATECOLUMNS;
    """.format(Bucket, FileName, key, secret)
    
    return query