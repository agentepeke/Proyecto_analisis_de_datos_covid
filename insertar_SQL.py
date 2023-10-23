import csv
import mysql.connector
from datetime import datetime


class InsertarSQL:
    def __init__(self, host, port, user, password, database, csv_file):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.csv_file = csv_file
        self.conexion = None  # Inicializa la conexión
        self.cursor = None  # Inicializa el cursor

    def insertar_datos(self, limite=1000000):
        try:
            # Conectar a la base de datos SQL
            self.conexion = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )

            # Crear el cursor después de la conexión
            self.cursor = self.conexion.cursor()

            # Abre el archivo CSV y procesa los datos
            with open(self.csv_file, 'r', encoding='utf-8') as archivo_csv:
                lector_csv = csv.DictReader(archivo_csv)
                contador = 0  # Contador para seguir la cantidad de filas insertadas
                for fila in lector_csv:
                    if contador >= limite:
                        break  # Salir del bucle después de alcanzar el límite de inserción
                    # Construye la sentencia de inserción con sentencias preparadas
                    consulta = "INSERT INTO covid_data (_id, age, city, country, recovery_test, region, sex) " \
                               "VALUES (%s, %s, %s, %s, %s, %s, %s)"

                    # Recoge los valores de la fila y reemplaza None con 'NULL'
                    valores = (
                        fila.get('_id', 'NULL'),
                        fila.get('age', 'NULL'),
                        fila.get('city', 'NULL'),
                        fila.get('country', 'NULL'),
                        fila.get('recovery_test', 'NULL'),
                        fila.get('region', 'NULL'),
                        fila.get('sex', 'NULL'),
                    )

                    # Inserta los datos en SQL utilizando sentencias preparadas
                    self.cursor.execute(consulta, valores)
                    self.conexion.commit()
                    contador += 1  # Incrementar el contador de filas insertadas

            # Imprime un mensaje para indicar que se han insertado todos los datos
            print(f"Se insertaron {contador} datos en SQL correctamente.")
        except mysql.connector.Error as mysql_error:
            print(f"Error al insertar datos en SQL: {mysql_error}")
        except Exception as e:
            print(f"Error general: {e}")
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conexion:
                self.conexion.close()
    def validar_fecha(self, fecha_str):
        try:
            # Intenta convertir la cadena a una fecha válida
            fecha = datetime.strptime(fecha_str, '%Y-%m-%d')  # Ajusta el formato según tus datos
            return fecha.strftime('%Y-%m-%d')  # Convierte la fecha de nuevo a cadena con formato
        except ValueError:
            # Si la conversión falla, maneja el error según tus necesidades
            return None  # Puedes devolver None u otro valor predeterminado
