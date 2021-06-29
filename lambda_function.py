import json
import Conexion
import re 

s3BucketName = 'cargamedicamentos'

def lambda_handler(event, context):
    print('inicio')
    con = Conexion.ConexionBD()
    bucket,s3 = Conexion.ConexionBucket(s3BucketName)
    
    print('Recorrer archivo')
    
    for s3_file in bucket.objects.all():
        fileName = s3_file.key
        if not (re.search('Cargado+', fileName)):
            
            print('Cargando')
            query = Conexion.GeneraQuery(fileName, s3BucketName) 
            cur = con.cursor();
            cur.execute("begin;")
            cur.execute(query)
            cur.execute("commit;")
            print("Copy executed fine!")
            
            destFileKey = 'Cargado_' + fileName
            copySource = s3BucketName + '/' + fileName          
            s3.Object(s3BucketName, destFileKey).copy_from(CopySource=copySource)
            print('Archivo copiado')
            s3.Object(s3BucketName, fileName).delete()
            print('Archivo Eliminado')
    
    con.close()
    return {
        'statusCode': 200,
        'body': json.dumps('Archivos Cargados')
    }


    