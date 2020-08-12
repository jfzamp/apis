import pymysql
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

## Proceso: Generar conexion a BD
## Descripcion: Desencripta, evalua y genera la conexion a la BD con los parametros recibidos
class Conexion_BD:

    ### Nos permite desencriptar los datos recibidos (Solamente los datos de la conexion) llamando al API
    def desencriptar(event):

        url = "https://amazonaws.com/bd_desencriptar_conexion"
        headers = {}
        # Hacemos el request 
        solicitud = requests.post(url, json=event, headers = headers)

        return solicitud

    ### Nos permite generar la cadena de conexion a la BD de la cual proviene la ejecucion
    def conexion(event):
        try:
            # Desencriptamos el diccionario
            r = Conexion_BD.desencriptar(event)
            event = r.json()
            # Evaluamos la respuesta de la API pero falta evaluar la respuesta de la funcion
            if r.status_code == 200:
                # Verificamos si se encuentra el nodo de conexion
                if 'conexion' in event:
                    engineapplication = create_engine("""mysql+pymysql://""" + str(event['conexion']['usuario']) + """:""" + str(event['conexion']['contrasena']) + """@""" + str(event['conexion']['instancia']) + """:""" + str(event['conexion']['puerto']) + """/""" + str(event['conexion']['bd']), poolclass=NullPool)
                    connection = engineapplication.connect()
                    resultado_conexion = connection.execute("""SELECT 1 AS is_alive""").fetchall()
                    connection.close()
                    resultado_conexion = True
                    return resultado_conexion, engineapplication
                else:
                    resultado_conexion = "API ERROR " + str(event)
                    engineapplication = None
                    return resultado_conexion, engineapplication
            else:
                resultado_conexion = "API ERROR HTTP"
                engineapplication = None
                return resultado_conexion, engineapplication
        except Exception as e: 
            resultado_conexion = e
            engineapplication = None
        return resultado_conexion, engineapplication
