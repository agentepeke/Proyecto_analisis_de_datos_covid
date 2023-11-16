import conexion_SQL
import conexion_aws
import insertar_aws
from conexion_mongo import ObtenerConexion
from insertar_SQL import InsertarSQL
from insertar_aws import InsertarAWS
import tkinter as tk
from tkinter import ttk
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
from pymongo import MongoClient
import pymongo
import boto3
import webbrowser
from rpy2 import robjects
from rpy2.robjects import pandas2ri
from sqlalchemy import create_engine


from insertar_mongo import InsertarMongo

aws_access_key_id="AKIARWNB3T23E3RTZCZD"
aws_secret_access_key="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"
region_name="us-east-2"
table_name="casos_covid"
archivo_csv = "covidfinal.csv"

host_sql = "localhost"
user_sql = "root"
password_sql = "agentepeke"
database_sql = "casos_covid"
port_sql = 3306
conexion_SQL = conexion_SQL.ConexionSQL(host="localhost", port=3306, user="root", password="agentepeke", database="casos_covid")

db_url = "mongodb://localhost:27017/"  # Cambia la URL de conexión según tu configuración
db_name = "casos_covid"
conexion_SQL.conectar()

#----------------------------------- INSERTAR EN SQL
#insercion_sql = InsertarSQL(host_sql, port_sql, user_sql, password_sql, database_sql, archivo_csv)
#insercion_sql.insertar_datos(limite=1000000)
# Ver todos los datos de la tabla covid_data
#conexion_mongo.ver_todos_los_datos()

# Cerrar la conexión
#conexion_SQL.cerrar_conexion()

#---------------------------------------------------------------------------
#conexion_aws = conexion_aws.ConexionAWS(
#    aws_access_key_id="AKIARWNB3T23E3RTZCZD",
#    aws_secret_access_key="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",
#    region_name="us-east-2",
#    table_name="casos_covid"
#)
#conexion_aws.conectar()
#----------------------------------- INSERTAR EN AWS
#insercion_aws = InsertarAWS(aws_access_key_id, aws_secret_access_key, region_name, table_name, archivo_csv)
#insercion_aws.insertar_datos(limite=1000000)
#insercion_aws.contar_elementos_tabla()
#----------------------------------- INSERTAR EN MONGO
#insercion_mongo = InsertarMongo(db_url, db_name, archivo_csv)
#insercion_mongo.insertar_datos(limite=1000000)

def generar_graficos_r():
    # Conectar a la base de datos MySQL
    engine = create_engine('mysql+pymysql://root:agentepeke@localhost/casos_covid')

    # Gráfico 1: Distribución de casos por edad
    query_age = "SELECT age, COUNT(*) AS case_count FROM covid_data GROUP BY age"
    df_age = pd.read_sql(query_age, engine)

    # top 10 edades
    df_age = df_age.sort_values(by='case_count', ascending=False).head(10)

    # Convertir DataFrame de pandas a objeto R
    pandas2ri.activate()
    r_df_age = pandas2ri.py2rpy(df_age)

    # Gráfico de barras para distribución de casos por edad
    robjects.r.assign("r_df_age", r_df_age)
    robjects.r("""
        barplot(r_df_age$case_count, names.arg=r_df_age$age, col="green", main="Top 10 Edades con más casos de COVID", xlab="Edad", ylab="Número de casos", las=2)
    """)

    input("Presiona ENTER para pasar a la siguiente gráfica...")

    # Gráfico 2: Distribución de casos por ciudad
    query_country = "SELECT country, COUNT(*) AS case_count FROM covid_data GROUP BY country"
    df_country = pd.read_sql(query_country, engine)

    # top 10 ciudades
    df_country = df_country.sort_values(by='case_count', ascending=False).head(10)

    # Convertir DataFrame de pandas a objeto R
    r_df_country = pandas2ri.py2rpy(df_country)

    # Gráfico de barras para distribución de casos por ciudad
    robjects.r.assign("r_df_country", r_df_country)
    robjects.r("""
        barplot(r_df_country$case_count, names.arg=r_df_country$country, col="orange", main="Top 10 paises con más casos de COVID", xlab="Ciudad", ylab="Número de casos", las=2)
    """)
    input("PRECIONA ENTER PARA PASAR A LA SIGUIENTE GRAFICA...")
    # Gráfico 3: Distribución de casos por género
    query_sex = "SELECT sex, COUNT(*) AS case_count FROM covid_data GROUP BY sex"
    df_sex = pd.read_sql(query_sex, engine)

    # Convertir DataFrame de pandas a objeto R
    r_df_sex = pandas2ri.py2rpy(df_sex)

    # Gráfico de barras para distribución de casos por género
    robjects.r.assign("r_df_sex", r_df_sex)
    robjects.r("""
        barplot(r_df_sex$case_count, names.arg=r_df_sex$sex, col="purple", main="Distribución de casos de COVID por Género", xlab="Género", ylab="Número de casos", las=2)
    """)
    input("PRECIONA ENTER PARA SALIR...")

    pandas2ri.deactivate()

def group_age(age):
    return age // 20 * 20


# Función para mostrar la gráfica de SQL
def show_graph():
    # Conectar a la base de datos MySQL
    db_connection = mysql.connector.connect(
        host="localhost", port=3306, user="root", password="agentepeke", database="casos_covid"
    )

    # Obtener datos de la tabla "covid_data"
    query = "SELECT age FROM covid_data"
    df = pd.read_sql(query, db_connection)

    # Agrupar edades en rangos de 25 en 25
    df['age_group'] = df['age'].apply(group_age)

    # Contar el número de registros en cada grupo
    age_counts = df['age_group'].value_counts().sort_index()

    # Crear la gráfica
    plt.bar(age_counts.index, age_counts.values, width=20, align='edge')
    plt.xlabel('Rango de Edad')
    plt.ylabel('Cantidad')
    plt.title('Distribución de Edades')

    # Mostrar la gráfica
    plt.show()

    # Cerrar la conexión a la base de datos
    db_connection.close()

# Función para mostrar la gráfica de AWS
def show_graph_aws():
    # Configurar las credenciales de AWS (reemplaza con tus propias credenciales)
    aws_access_key_id = 'AKIARWNB3T23E3RTZCZD'
    aws_secret_access_key = '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'
    region_name = 'us-east-2'

    # Crear un cliente de DynamoDB
    dynamodb = boto3.client('dynamodb', aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key, region_name=region_name)

    # Escanear la tabla "casos_covid" en DynamoDB
    response = dynamodb.scan(
        TableName='casos_covid'
    )

    # Obtener los datos y convertirlos a un DataFrame de Pandas
    items = response['Items']
    data = [{'sex': item['sex']['S']} for item in items]
    df = pd.DataFrame(data)

    # Reemplazar valores nulos (None) con "Null"
    df['sex'].fillna('Desconocido', inplace=True)

    # Contar el número de registros en cada categoría
    sex_counts = df['sex'].value_counts()

    # Crear la gráfica de barras
    sex_counts.plot(kind='bar', width=0.8)
    plt.xlabel('Sexo')
    plt.ylabel('Cantidad')
    plt.title('Distribución de Género')
    plt.xticks(rotation=0)

    # Mostrar la gráfica
    plt.show()

# Función para obtener datos de MongoDB y mostrar la gráfica
def show_mongodb_graph():
    # Configurar la conexión a MongoDB
    client = MongoClient("mongodb://localhost:27017/")  # Reemplaza con la URL de tu servidor MongoDB si es diferente
    db = client.casos_covid  # Reemplaza con el nombre de tu base de datos
    collection = db.covid_data  # Reemplaza con el nombre de tu colección

    # Obtener los datos de MongoDB
    cursor = collection.aggregate([
        {
            "$group": {
                "_id": "$country",
                "count": {"$sum": 1}
            }
        },
        {
            "$match": {
                "_id": {"$ne": None}  # Excluir valores None
            }
        },
        {
            "$sort": {"count": -1}
        },
        {
            "$limit": 5  # Seleccionar solo los primeros 5 países
        }
    ])

    # Convertir los datos en un DataFrame de Pandas
    df = pd.DataFrame(list(cursor))

    if not df.empty:
        # Crear la gráfica de barras
        plt.bar(df["_id"], df["count"])
        plt.xlabel('País')
        plt.ylabel('Cantidad')
        plt.title('Casos de COVID-19 por País (Top 5)')
        plt.xticks(rotation=90)  # Rotar las etiquetas del eje x para mayor legibilidad

        # Mostrar la gráfica
        plt.show()
    else:
        print("No se encontraron datos para graficar.")
def abrir_url():
    url = "http://localhost:8080/pentaho"
    chrome_path = "C:\Program Files\Google\Chrome\Application\chrome.exe"  # Reemplaza con la ruta correcta
    webbrowser.register('chrome', None, webbrowser.BackgroundBrowser(chrome_path))
    webbrowser.get("chrome").open(url)


# Crear la ventana principal
root = tk.Tk()
root.title('Gráfica de Distribución de Edades')

# Crear un botón para mostrar la gráfica
show_button = ttk.Button(root, text='Mostrar Gráfica de Rango de Edades con la Base de datos de MySQL', command=show_graph)
show_button.pack()

# Crear un botón para mostrar la gráfica
show_buttonaws = ttk.Button(root, text='Mostrar Gráfica de Cantidad de Casos por Sexo de AWS', command=show_graph_aws)
show_buttonaws.pack()

# Crear un botón para mostrar la gráfica
show_button = ttk.Button(root, text='Mostrar Gráfica de los 5 Países con más Casos de MongoDB', command=show_mongodb_graph)
show_button.pack()

# Crear un botón
boton = tk.Button(root, text="Abrir URL en Chrome", command=abrir_url)
boton.pack(pady=20)

 #Crear un botón para ejecutar el código en R
r_button = ttk.Button(root, text='Generar Gráficos en R', command=generar_graficos_r)
r_button.pack()

# Iniciar la interfaz gráfica
root.mainloop()
