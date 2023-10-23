import csv
from pymongo import MongoClient

class InsertarMongo:
    def __init__(self, db_url, db_name, csv_file):
        self.db_url = db_url
        self.db_name = db_name
        self.csv_file = csv_file

    def insertar_datos(self, limite=1000000):
        try:
            # Conectar a la base de datos MongoDB
            client = MongoClient(self.db_url)
            db = client[self.db_name]
            collection = db["covid_data"]

            # Abre el archivo CSV y procesa los datos
            with open(self.csv_file, 'r') as archivo_csv:
                lector_csv = csv.DictReader(archivo_csv)
                contador = 0  # Contador para seguir la cantidad de filas insertadas
                for fila in lector_csv:
                    if contador >= limite:
                        break  # Salir del bucle después de alcanzar el límite de inserción
                    # Filtra y selecciona los campos que deseas insertar
                    datos = {
                        'age': fila.get('age', None),
                        'city': fila.get('city', None),
                        'confirmed_date': fila.get('confirmed_date', None),
                        'country': fila.get('country', None),
                        'date_onset_symptoms': fila.get('date_onset_symptoms', None),
                        'deceased_date': fila.get('deceased_date', None),
                        'infected_by': fila.get('infected_by', None),
                        'recovery_test': fila.get('recovery_test', None),
                        'region': fila.get('region', None),
                        'sex': fila.get('sex', None),
                        'smoking': fila.get('smoking', None),
                        'deceased_date_D': fila.get('deceased_date_D', None)
                    }

                    # Inserta los datos en la colección MongoDB
                    collection.insert_one(datos)
                    print("Datos insertados en MongoDB correctamente.")
                    contador += 1  # Incrementar el contador de filas insertadas

        except Exception as e:
            print(f"Error al insertar datos en MongoDB: {e}")