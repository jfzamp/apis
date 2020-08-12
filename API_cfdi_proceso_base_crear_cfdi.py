from sqlalchemy import text
from Conexion_BD import Conexion_BD
from decimal import Decimal
import requests
import json

#####
## Fecha de creacion: 15/Abril/2020
## Actualización: 01/Junio/2020
#####

### Proceso: Validar_parametros
### Descripcion: Valdia que lso apramtros no sean "" y si hay os cmabia a None
def validar_paramentros(event):
    i = 0
    for cfdi in event['parametros']['cfdi']:
        #claves_cfdi = cfdi.keys()
        for clave, valor in cfdi.items(): 
            if valor == "" or valor == "None" or valor == "none":
                 event['parametros']['cfdi'][i][clave] = None

        ## evaluamos si tienne aprtidas relacionadas
        if 'partida' in cfdi:
            p = 0
            for partida in cfdi['partida']:
                #claves_partida = cfdi['partida'].keys()
                for clave, valor in partida.items(): 
                    if valor == "" or valor == "None" or valor == "none":
                         event['parametros']['cfdi'][i]['partida'][p][clave] = None
                p = int(p) + int(1)

        ## evaluamos si tienne documentos relacioandos 
        if 'documentos_relacionados' in cfdi:
            d = 0
            for documento in cfdi['documentos_relacionados']:
                #claves_partida = cfdi['partida'].keys()
                for clave, valor in documento.items(): 
                    if valor == "" or valor == "None" or valor == "none":
                         event['parametros']['cfdi'][i]['documentos_relacionados'][d][clave] = None
                d = int(d) + int(1)
        i = int(i) + int(1)

    return event

### Proceso: Crear CFDI
### Se llama a la API encargada de obtener los datos de la empresa
def obtener_datos_empresa(event):

    parametros_datos_empresa = {
                        "conexion": {
                            "usuario": event['conexion']['usuario'],
                            "contrasena": event['conexion']['contrasena'],
                            "instancia": event['conexion']['instancia'],
                            "puerto": event['conexion']['puerto'],
                            "bd": event['conexion']['bd'],
                            "nombre_app": event['conexion']['nombre_app'],
                            "fk_user": event['conexion']['fk_user']
                          },
                        "parametros": {
                            "id_emisor": str(event['parametros']['id_emisor']),
                            "id_domicilio_fiscal": str(event['parametros']['id_emisor'])
                        }
                     }

    url = "https://amazonaws.com/bd_empresa"
    headers = {}
    # Hacemos el request 
    solicitud = requests.post(url, json = parametros_datos_empresa, headers = headers)

    solicitud_datos_empresa = solicitud.json()

    return solicitud_datos_empresa

### Proceso: Obtner datos arendatario
### Se llama a la API encargada de obtener los datos del arrendatario
def obtener_datos_arrendatario(event):

    parametros_datos_arrendatario = {
                    "conexion": {
                        "usuario": event['conexion']['usuario'],
                        "contrasena": event['conexion']['contrasena'],
                        "instancia": event['conexion']['instancia'],
                        "puerto": event['conexion']['puerto'],
                        "bd": event['conexion']['bd'],
                        "nombre_app": event['conexion']['nombre_app'],
                        "fk_user": event['conexion']['fk_user']
                      },
                    "parametros": {
                        "id_arrendatario": str(event['parametros']['id_arrendatario']),
                        "id_sucursal": str(event['parametros']['id_sucursal'])
                    }
                 }

    url = "https://amazonaws.com/bd_arrendatario"
    headers = {}
    # Hacemos el request 
    solicitud = requests.post(url, json = parametros_datos_arrendatario, headers = headers)

    solicitud_datos_arrendatario = solicitud.json()
    return solicitud_datos_arrendatario

### Proceso: Enviar Api agregar partida
### Descripcion: Se llama a la API encargada de crear la partida
def llamar_agregar_partida(diccionario):
    try:

        # Definimos la cabecera y el diccionario con los datos
        url = "https://amazonaws.com/bd_partida"
        headers = {}
        # Hacemos el request 
        solicitud = requests.post(url, json = diccionario, headers = headers)
    
        resultado_agregar_partida = solicitud.json()

        # Si se obtuvo un proceso correcto
        if 'result' in resultado_agregar_partida:
            return resultado_agregar_partida
        else:
            response = {
                        "result": {
                            "process": False,
                            "data":{}
                        }, 
                        "errors": "Error API: " + str(resultado_agregar_partida['errorMessage'])
                        }
            return response

    except Exception as e:
      
            response = {
                        "result": {
                            "process": False,
                            "data":{}
                        }, 
                        "errors": "Error al enviar " + str(e)
                        }
            return response

### Proceso: Obtiene parametros CFDI
### Descripcion: Funcion encargada de obtener el registro de parametros de cfdi general
def obtener_parametros_cfdi(engineapplication):
     
    query_select = text("""SELECT 
                           fk_variable_pendiente, 
                           fk_variable_no_aplica, 
                           fk_documento_nota_credito,
                           fk_cobranza_regular
                           FROM parametro_cfdi """)
    
    obtener_parametros_cfdi = engineapplication.execute(query_select).fetchone() 
    
    return obtener_parametros_cfdi

### Proceso: Crear CFDI
### Descripcion: Proceso encargado de generar un nuevo CFDI con los datos de entrada y los obtenidos de empresa y arrendatario
def crear_cfdi(engineapplication, event, datos_empresa, datos_arrendatario):

    try:
        listado_errores = ""
        #Se ejecuta el query para cada CFDI en la lista
        for cfdi in event['parametros']['cfdi']:

            query_crear_cfdi = text("""INSERT INTO `cfdi` (
                                `createdUserKey`, 
                                `createdDate`,
                                `fk_TBempresaTB_alias`,
                                `fk_TBarrendatarioTB_alias`,
                                `fk_TBdomicilio_fiscalTB_alias`,
                                `razon_emisor`,
                                `rfc_emisor`,
                                `fk_TBregimen_fiscalTB_clave`,
                                `lugar_expedicion`,
                                `direccion_emisor`,
                                `razon_receptor`,
                                `sucursal`,
                                `rfc_receptor`,
                                `fk_TBforma_pagoTB_clave`,
                                `fk_TBmetodo_pagoTB_clave`,
                                `fk_TBuso_cfdiTB_clave`,
                                `enviar_factura`,
                                `direccion_receptor`,
                                `fk_TBinmuebleTB_inmueble`,
                                `fk_TBunidadTB_unidad`,
                                `fk_TBcontratoTB_folio`,
                                `fecha`,
                                `fecha_limite`,
                                `fk_TBestatus_documentoTB_estatus_documento`,
                                `fk_TBtipo_documentoTB_tipo_documento`,
                                `fk_TBgrupo_avisoTB_grupo_aviso`,
                                `titulo`,
                                `fk_TBestatus_timbreTB_estatus_timbre`,
                                `fk_TBestatus_cobranzaTB_estatus_cobranza`,
                                `fk_TBestatus_variableTB_estatus_variable`,
                                `fk_TBestatus_conectorTB_estatus_conector`,
                                `fk_TBestatus_conectorTB_estatus_cancelacion`,
                                `fk_TBmonedaTB_moneda`,
                                `tipo_cambio`,
                                `observaciones`
                            )
                            VALUES
                            (
                                :createdUserKey, 
                                NOW(), 
                                :fk_TBempresaTB_alias,
                                :fk_TBarrendatarioTB_alias,
                                :fk_TBdomicilio_fiscalTB_alias,
                                :razon_emisor,
                                :rfc_emisor,
                                :fk_TBregimen_fiscalTB_clave,
                                :lugar_expedicion,
                                :direccion_emisor,
                                :razon_receptor,
                                :sucursal,
                                :rfc_receptor,
                                :fk_TBforma_pagoTB_clave,
                                :fk_TBmetodo_pagoTB_clave,
                                :fk_TBuso_cfdiTB_clave,
                                :enviar_factura,
                                :direccion_receptor,
                                :fk_TBinmuebleTB_inmueble,
                                :fk_TBunidadTB_unidad,
                                :fk_TBcontratoTB_folio,
                                :fecha,
                                :fecha_limite,
                                :fk_TBestatus_documentoTB_estatus_documento,
                                :fk_TBtipo_documentoTB_tipo_documento,
                                :fk_TBgrupo_avisoTB_grupo_aviso,
                                :titulo,
                                :fk_TBestatus_timbreTB_estatus_timbre,
                                :fk_TBestatus_cobranzaTB_estatus_cobranza,
                                :fk_TBestatus_variableTB_estatus_variable,
                                :fk_TBestatus_conectorTB_estatus_conector,
                                :fk_TBestatus_conectorTB_estatus_cancelacion,
                                :fk_TBmonedaTB_moneda,
                                :tipo_cambio,
                                :observaciones
                                );""")

            parametros = obtener_parametros_cfdi(engineapplication)

            # Se valida que existan parametros
            if parametros is not None:

                if 'id_estatus_variable' in cfdi:
                    id_estatus_variable = cfdi['id_estatus_variable']
                else:
                    id_estatus_variable = parametros['fk_variable_no_aplica']

                if cfdi['id_estatus_cobranza'] is None:
                    cfdi['id_estatus_cobranza'] = parametros['fk_cobranza_regular']

                if cfdi['tipo_cambio'] is None:
                    ## Importante que sea 1 y NO 0
                    tipo_cambio = 1
                else:
                    tipo_cambio = cfdi['tipo_cambio']

                #Se obtiene el id del nuevo CFDI
                id_nuevo_cfdi =engineapplication.execute(query_crear_cfdi, 
                                                        createdUserKey = str(event['conexion']['fk_user']),
                                                        fk_TBempresaTB_alias = cfdi['id_emisor'],
                                                        fk_TBdomicilio_fiscalTB_alias = cfdi['id_domicilio_fiscal'],
                                                        razon_emisor = datos_empresa['result']['data']['informacion_empresa']['razon_social'],
                                                        rfc_emisor = datos_empresa['result']['data']['informacion_empresa']['rfc_emisor'],
                                                        fk_TBregimen_fiscalTB_clave = datos_empresa['result']['data']['informacion_domicilio_fiscal']['regimen_fiscal_id'],
                                                        lugar_expedicion = datos_empresa['result']['data']['informacion_domicilio_fiscal']['codigo_postal'],
                                                        direccion_emisor = datos_empresa['result']['data']['informacion_domicilio_fiscal']['direccion'],
                                                        fk_TBarrendatarioTB_alias = cfdi['id_receptor'],
                                                        sucursal = cfdi['id_sucursal'],
                                                        razon_receptor = datos_arrendatario['result']['data']['informacion_arrendatario']['razon_social'],
                                                        rfc_receptor = datos_arrendatario['result']['data']['informacion_arrendatario']['rfc_receptor'],
                                                        fk_TBforma_pagoTB_clave = datos_arrendatario['result']['data']['informacion_sucursal']['forma_pago_id'],
                                                        fk_TBmetodo_pagoTB_clave = datos_arrendatario['result']['data']['informacion_sucursal']['metodo_pago_id'],
                                                        fk_TBuso_cfdiTB_clave = datos_arrendatario['result']['data']['informacion_sucursal']['uso_cfdi_id'],
                                                        enviar_factura = datos_arrendatario['result']['data']['informacion_sucursal']['enviar_cfdi'],
                                                        direccion_receptor = datos_arrendatario['result']['data']['informacion_sucursal']['direccion'],
                                                        fk_TBinmuebleTB_inmueble = cfdi['id_inmueble'],
                                                        fk_TBunidadTB_unidad = cfdi['id_unidad'],
                                                        fk_TBcontratoTB_folio = cfdi['id_contrato'],
                                                        fecha = cfdi['fecha'],
                                                        fecha_limite = cfdi['fecha_limite_pago'],
                                                        fk_TBtipo_documentoTB_tipo_documento = cfdi['id_tipo_documento'],
                                                        fk_TBgrupo_avisoTB_grupo_aviso = cfdi['id_grupo_avisos'],
                                                        titulo = cfdi['titulo'],
                                                        fk_TBestatus_documentoTB_estatus_documento = cfdi['id_estatus'],
                                                        fk_TBestatus_timbreTB_estatus_timbre = cfdi['id_estatus_timbre'],
                                                        fk_TBestatus_variableTB_estatus_variable = id_estatus_variable,
                                                        fk_TBestatus_cobranzaTB_estatus_cobranza = cfdi['id_estatus_cobranza'],
                                                        fk_TBestatus_conectorTB_estatus_conector = cfdi['conector'],
                                                        fk_TBestatus_conectorTB_estatus_cancelacion = cfdi['conector_cancelacion'],
                                                        fk_TBmonedaTB_moneda = cfdi['id_moneda'],
                                                        tipo_cambio =  tipo_cambio,
                                                        observaciones = cfdi['observaciones']).lastrowid

                # Se valida si se genero correctamente el registro
                if id_nuevo_cfdi is not None:
                    # CRea un CFDI relacioando solo si tiene valores en el array...
                    if 'documentos_relacionados' in cfdi:
                        if len(cfdi['documentos_relacionados']) > 0:
                            resultado_crear_cfdi_relacionado = crear_cfdi_relacionado(engineapplication, cfdi['documentos_relacionados'], id_nuevo_cfdi, event['conexion']['fk_user'])
                    
                    # Verificamos que el nodo partidas si tenga algun registro de lo contrario se acab el proceso con el CFDI en ceros peor NO marcamos ERROR
                    if len(cfdi['partida']) > 0:
                        # Se crean las partidas y se retornan los datos para actualizar el CFDI
                        resultado_crear_partida = crear_partida(event, engineapplication, cfdi['partida'], parametros, cfdi['id_tipo_documento'], cfdi['tipo_cambio'], id_nuevo_cfdi, event['conexion']['fk_user'])

                        #Valida que la creacion de la aprtida sea correcta de loc ontratrio continua la lista de errores..
                        if resultado_crear_partida['result']['process'] == False:
                            listado_errores = listado_errores +  "No pudimos crear las partidas: " + str(resultado_crear_partida['errors'])

                # De lo contrario se manda mensaje de error al generar un CFDI
                else:
                    listado_errores = listado_errores + "Error al generar CFDI de titulo: " + str(cfdi['titulo']) + "\n"
            
            else:
                listado_errores = listado_errores +  "No pudimos crear las partidas, No se obtuvieron parametros del CFDI: " + str(parametros) 

        
        # Verificamos que no hay ocurrido ningun error  al geenra los cfdi de esta forma podemos tenr una lista de errores mas detallada
        if listado_errores == "" or listado_errores is None:
            response = {
                        "result": {
                            "process": True,
                            "data": id_nuevo_cfdi,
                            },
                        "errors":{
                            
                            }
                    }
        else:
            response = {
                        "result": {
                            "process": False,
                            "data": [],
                            },
                        "errors": str(listado_errores)
                    }

        return response

    except Exception as e:
        response = {
                        "result": {
                            "process": False,
                            "data": [],
                            },
                        "errors": "Excepcion crear CFDI: " + str(e)
                    }
        return response

### Proceso: Crear Partida
### Descripcion: Proceso encargado de generar partidas correspondientes a un nuevo CFDI 
def crear_partida(event,engineapplication, partidas, parametros, id_tipo_documento,tipo_cambio_cfdi, id_cfdi, fk_user):

    try:
        # Se ejecuta el query para cada elemento de la lista de partidas
        for partida in partidas:

            diccionario = {
                "conexion": {
                    "usuario": event['conexion']['usuario'],
                    "contrasena": event['conexion']['contrasena'],
                    "instancia": event['conexion']['instancia'],
                    "puerto": event['conexion']['puerto'],
                    "bd": event['conexion']['bd'],
                    "nombre_app": event['conexion']['nombre_app'],
                    "fk_user": event['conexion']['fk_user']
                    },
                "parametros": {
                    "id_cfdi": id_cfdi,
                    "descripcion": partida['descripcion'],
                    "cuenta_predial": partida['cuenta_predial'],
                    "id_concepto": partida['id_concepto'],
                    "id_inmueble": partida['id_inmueble_partida'],
                    "id_unidad": partida['id_unidad_partida'],
                    "id_impuesto": partida['id_grupo_impuesto'],
                    "valor_unitario": partida['valor_unitario'],
                    "cantidad": partida['cantidad'],
                    "tipo_cambio": tipo_cambio_cfdi,
                    "descuento": partida['descuento']
                    }
                }

            nueva_partida =  llamar_agregar_partida(diccionario)

            if nueva_partida['result']['process'] == True:
                response = {
                            "result": {
                                "process": True,
                                "data": [],
                                },
                            "errors":""
                        }
            else:
                response = {
                    "result": {
                        "process": False,
                        "data": [],
                        },
                    "errors":{
                        "Error al generar partida: " + str(nueva_partida['errors']) 
                        }
                }

        return response

    except Exception as e:
        response = {
                    "result": {
                        "process": False,
                        "data": []
                        },
                    "errors": "Excepcion en Partida: " + str(e)
                    }
        return response
 
### Proceso: Crear CFDI 
### Descripcion: Proceso encargado crear un CFDI relacionado con los datos del CFDI creado
def crear_cfdi_relacionado(engineapplication, documentos_relacionados, id_cfdi, fk_user):

    try:
        for documento in documentos_relacionados:
            if documento["uuid_interno"] != "" and documento["uuid_interno"] is not None and documento['relacion_original'] != "" and documento['relacion_original'] is not None:
                # Se genera query para generar nuevo registro

                query_crear_cfdi_relacionado = text("""INSERT INTO `cfdi_relacionado` (
                                    `createdUserKey`, 
                                    `createdDate`, 
                                    `fk_TBtipo_relacionTB_clave`,
                                    `origen_folio`,
                                    `fk_TBcfdiTB_folio_fiscal`,
                                    `uuid_externo`,
                                    `fk_TBcfdiTB_id_cfdi`
                                    )
                                    VALUES
                                    (
                                    :createdUserKey,
                                    NOW(),
                                    :fk_TBtipo_relacionTB_clave,
                                    :origen_folio,
                                    :fk_TBcfdiTB_folio_fiscal,
                                    :uuid_externo,
                                    :fk_TBcfdiTB_id_cfdi
                                    );""")

                engineapplication.execute(query_crear_cfdi_relacionado, 
                                          createdUserKey = str(fk_user), 
                                          fk_TBtipo_relacionTB_clave = documento['relacion_original'],
                                          origen_folio = None,
                                          fk_TBcfdiTB_folio_fiscal = id_cfdi,
                                          uuid_externo = documento['uuid_interno'],
                                          fk_TBcfdiTB_id_cfdi = id_cfdi)
                response = {
                                "result": {
                                    "process": True,
                                    "data": "Se creo el CFDI Relacionado",
                                    },
                                "Error":{}
                            }
            else:
                response = {
                                "result": {
                                    "process": True,
                                    "data": "No se creo el CFDI Relacionado",
                                    },
                                "Error":{}
                            }
         
        return response  

    except Exception as e:

        response = {
                    "result": {
                        "process": False,
                        "data": []
                        },
                    "errors": "Error al crear el CFDI relacionado: " + str(e)
                    }

        return response

### Proceso: Proceso principal encargado de invocar las funciones nesesarias
### Descripcion: Se encarga de ir invocando las funciones nesesarias        
def proceso(engineapplication, event):
    try:
        # Valdiar paramtros
        event = validar_paramentros(event)
        # Se obtienen datos de empresa
        datos_empresa = obtener_datos_empresa(event)  

        # Se valida que exitan datos      
        if datos_empresa is not None:
            
            # Se obtienen datos de arrendatario
            datos_arrendatario = obtener_datos_arrendatario(event)
            
            # Se valida que exitan datos      
            if datos_arrendatario is not None:

                # Se crea el CFDI y se obtiene el id
                resultado_crear_cfdi = crear_cfdi(engineapplication, event, datos_empresa, datos_arrendatario)

                # Se valida que se haya creado correctamente el CFDI
                if resultado_crear_cfdi['result']['process'] == True:

                    response = {
                        "result": {
                            "process": True,
                            "data": {
                                "id_cfdi": resultado_crear_cfdi['result']['data']
                                    }
                                },
                            "errors": []
                        }
                else:
                    response = {
                        "result": {
                            "process": False,
                            "data": [],
                            },
                        "errors":{
                            "Error al crear CFDI: " + str(resultado_crear_cfdi['errors']) 
                            }
                    }
            else:
                response = {
                        "result": {
                            "process": False,
                            "data": [],
                            },
                        "errors":{
                            "No se obtuvieron datos de arrendatario" 
                            }
                    }
        else: 
            response = {
                        "result": {
                            "process": False,
                            "data": [],
                            },
                        "errors":{
                            "No se obtuvieron datos de empresa" 
                            }
                    }

        return response                

    except Exception as e:
        response = {
                    "result": {
                        "process": False,
                        "data": []
                        },
                    "errors": "Error en proceso  " + str(e)
                    }
        return response

### Proceso: Inicializa la ejecucion
### Descripcion: Se encarga de recibir los parametros y llevar el camino del proceso hasta retornar un resultado
def main(event, context):

    ### Desencriptamos y generamos cadena de conexion
    conexion_bd = Conexion_BD
    (resultado_conexion, engineapplication) = conexion_bd.conexion(event)
    
    ### Si el resultado de la conexion fue valido, se realiza proceso
    if resultado_conexion == True:

        response = proceso(engineapplication, event)
        print(f"response: {response}")
        return response

    ### De lo contrario enviamos error
    else:
        response = {
                    "result": {
                        "process": False,
                        "data": "Error conexion bd: " + str(resultado_conexion)
                        },
                    "errors": []
                    }
        #print(f"response: {response}")
        return response
        
# # ### Simula el envio del json
contenido_json = {
   "conexion": {
        "usuario": "master",
        "instancia": "xxxxx",
        "contrasena": "xxxxx",
        "puerto": "xx",
        "bd": "db",
        "nombre_app": "proyecto",
        "fk_user": "usuario"
       }, 
       "parametros": {
            "id_emisor": 1, 
            "id_arrendatario": 1, 
            "id_domicilio_fiscal": 1, 
            "id_sucursal": 1, 
            "cfdi": [{
                "id_emisor": 1, 
                "id_domicilio_fiscal": 1, 
                "id_receptor": 1, 
                "id_sucursal": 1, 
                "id_inmueble": 1, 
                "id_unidad": 1, 
                "id_contrato": 1, 
                "id_grupo_avisos": 1, 
                "fecha": "2020-05-07", 
                "fecha_limite_pago": None, 
                "id_tipo_documento": 5, 
                "titulo": "Nota de credito", 
                "id_estatus": 3, 
                "id_estatus_timbre": 1, 
                "id_estatus_cobranza": 1, 
                "conector": 3, 
                "conector_cancelacion": 4, 
                "id_moneda": None, 
                "tipo_cambio": 1.0, 
                "observaciones": None, 
                "partida": [{
                    "id_concepto": 1, 
                    "descripcion": "descrip",
                    "Nota de credito": None, 
                    "cuenta_predial": None, 
                    "id_inmueble_partida": 1, 
                    "id_unidad_partida": 1, 
                    "estatus_variable": 3, 
                    "id_grupo_impuesto": 1, 
                    "valor_unitario": "50", 
                    "cantidad": 1, 
                    "descuento": 0, 
                    "id_tipo_variable": None, 
                    "fecha_variable": None, 
                    "minimo_variable": None, 
                    "porcentaje_variable": None
                    }], 
                "documentos_relacionados": [{
                    "uuid_interno": "5", 
                    "relacion_original": 7}]
                }]
            }
        }

context = ""

main(contenido_json, context)
