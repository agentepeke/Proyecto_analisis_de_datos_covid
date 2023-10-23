
        #aws_access_key_id="AKIARWNB3T23E3RTZCZD",
        #aws_secret_access_key="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",
        #region_name="us-east-2"
import boto3

class ConexionAWS:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, table_name):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.table_name = table_name
        self.dynamodb = None
        self.id_counter = "0"  # Variable como cadena para realizar un seguimiento del número

    def conectar(self):
        try:
            self.dynamodb = boto3.client('dynamodb',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
            print("Conexión a AWS DynamoDB establecida.")
        except Exception as e:
            print(f"Error al conectar a AWS DynamoDB: {e}")

    def agregar_datos(self, datos):
        if self.dynamodb is not None:
            try:
                self.id_counter = str(int(self.id_counter) + 1)  # Incrementa el contador y convierte a cadena
                id_covid = f'{self.id_counter}'  # Crea la cadena de "Id_covid"
                response = self.dynamodb.put_item(
                    TableName=self.table_name,
                    Item={
                        'Id_covid': {'S': id_covid},  # Cambia a tipo cadena (S)
                        'age': {'N': str(datos[0])},
                        'city': {'S': datos[1]},
                        'confirmed_date': {'S': datos[2]},
                        'country': {'S': datos[3]},
                        'date_onset_symptoms': {'S': datos[4]},
                        'deceased_date': {'S': datos[5]},
                        'infected_by': {'N': str(datos[6])},
                        'recovery_test': {'S': datos[7]},
                        'region': {'S': datos[8]},
                        'sex': {'S': datos[9]},
                        'smoking': {'S': datos[10]},
                        'deceased_date_D': {'S': datos[11]}
                    }
                )
                print("Datos agregados correctamente a AWS DynamoDB.")
            except Exception as e:
                print(f"Error al agregar datos a AWS DynamoDB: {e}")

    def listar_todos_los_datos(self):
        if self.dynamodb is not None:
            try:
                response = self.dynamodb.scan(
                    TableName=self.table_name
                )
                items = response.get('Items', [])
                for item in items:
                    print(item)
            except Exception as e:
                print(f"Error al listar datos en AWS DynamoDB: {e}")