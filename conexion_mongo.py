import pymongo

def ObtenerConexion():
    """Devuelve la conexion a la base de datos"""
    MONGO_HOST="root" # user MongoDB
    MONGO_PASSWORD="12345"  # password MongoDB
    MONGO_TIEMPO_FUERA=1000 # Tiempo de espera en milisegundos

    MONGO_URI="mongodb+srv://"+MONGO_HOST+":"+MONGO_PASSWORD+"@atlascluster.vef8xhy.mongodb.net/"  # URI de conexión

    
    MONGO_BASEDATOS="pacientes" # Nombre de la base de datos
    MONGO_COLECCION="paciente" # Nombre de la colección

    try:
        cliente = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIEMPO_FUERA) 
        cliente.server_info()  # Intenta acceder a la información del servidor

        # Accede a la base de datos y la colección
        baseDatos = cliente[MONGO_BASEDATOS]  
        coleccion = baseDatos[MONGO_COLECCION]  
        
        print("Conexión a MongoDB exitosa.")
        
        return coleccion
    
    except pymongo.errors.ServerSelectionTimeoutError:
        print("Error: No se pudo conectar a MongoDB. Verifica la configuración.")
        return None
