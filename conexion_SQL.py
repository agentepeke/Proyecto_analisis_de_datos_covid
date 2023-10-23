import mysql.connector


class ConexionSQL:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conexion = None

    def conectar(self):
        try:
            self.conexion = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.conexion.is_connected():
                print("Conexión a la base de datos establecida.")
        except mysql.connector.Error as error:
            print(f"Error al conectar a la base de datos: {error}")

    def agregar_datos(self, datos):
        if self.conexion is not None and self.conexion.is_connected():
            try:
                cursor = self.conexion.cursor()
                consulta = "INSERT INTO covid_data (age, city, confirmed_date, country, date_onset_symptoms, deceased_date, infected_by, recovery_test, region, sex, smoking, deceased_date_D) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(consulta, datos)
                self.conexion.commit()
                print("Datos agregados correctamente.")
            except mysql.connector.Error as error:
                print(f"Error al agregar datos: {error}")

    def ver_todos_los_datos(self):
        if self.conexion is not None and self.conexion.is_connected():
            try:
                cursor = self.conexion.cursor()
                consulta = "SELECT * FROM covid_data"
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                for fila in resultados:
                    print(fila)
            except mysql.connector.Error as error:
                print(f"Error al recuperar datos: {error}")

    def cerrar_conexion(self):
        if self.conexion is not None and self.conexion.is_connected():
            self.conexion.close()