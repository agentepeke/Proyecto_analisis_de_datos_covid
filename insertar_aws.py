import csv
import boto3

class InsertarAWS:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, table_name, csv_file):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.table_name = table_name
        self.csv_file = csv_file
        self.last_id_covid = 0

    def obtener_ultimo_id_covid(self):
        # Implementa la lógica para obtener el último valor de Id_covid desde DynamoDB
        # Debe adaptarse a tu estructura de datos en DynamoDB

        # Ejemplo:
        # response = self.dynamodb.scan(
        #     TableName=self.table_name,
        #     Select='COUNT'
        # )
        # last_id_covid = response['Count'] + 1
        # return last_id_covid

        # En lugar de la lógica real, usamos un contador simple para demostrar
        self.last_id_covid += 1
        return self.last_id_covid

    def insertar_datos(self, limite=1000000):
        try:
            # Conectar a AWS DynamoDB
            self.dynamodb = boto3.client(
                'dynamodb',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )

            # Abre el archivo CSV y procesa los datos
            with open(self.csv_file, 'r') as archivo_csv:
                lector_csv = csv.DictReader(archivo_csv)
                contador = 0  # Contador para seguir la cantidad de filas insertadas
                for fila in lector_csv:
                    if contador >= limite:
                        break  # Salir del bucle después de alcanzar el límite de inserción
                    # Obtén el próximo valor único para Id_covid
                    id_covid = self.obtener_ultimo_id_covid()

                    # Construye el registro a insertar
                    datos = {
                        'Id_covid': {'S': f'A{id_covid}'},
                        'age': {'N': fila.get('age', '0')},
                        # Puedes cambiar '0' por otro valor predeterminado si lo deseas
                        'city': {'S': fila.get('city', '')},
                        'confirmed_date': {'S': fila.get('confirmed_date', '')},
                        'country': {'S': fila.get('country', '')},
                        'date_onset_symptoms': {'S': fila.get('date_onset_symptoms', '')},
                        'deceased_date': {'S': fila.get('deceased_date', '')},
                        'infected_by': {'N': fila.get('infected_by', '0')},
                        # Puedes cambiar '0' por otro valor predeterminado si lo deseas
                        'recovery_test': {'S': fila.get('recovery_test', '')},
                        'region': {'S': fila.get('region', '')},
                        'sex': {'S': fila.get('sex', '')},
                        'smoking': {'S': fila.get('smoking', '')},
                        'deceased_date_D': {'S': fila.get('deceased_date_D', '')},
                    }

                    # Inserta los datos en la tabla DynamoDB
                    self.dynamodb.put_item(
                        TableName=self.table_name,
                        Item=datos
                    )
                    contador += 1  # Incrementar el contador de filas insertadas

            print(f"Se insertaron {contador} datos en AWS DynamoDB.")
        except Exception as e:
            print(f"Error al insertar datos en AWS DynamoDB: {e}")