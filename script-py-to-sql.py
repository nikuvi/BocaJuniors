import pandas as pd
from sqlalchemy import create_engine, text
import os

class DriversToSQL:
    def __init__(self, server="NIKU\\SQLEXPRESS", database="Formula1"):
        """
        Conexión específica para migrar drivers.csv
        """
        # Formato correcto para SQLAlchemy con instancia nombrada
        if '\\' in server:
            # Para instancias nombradas como NIKU\SQLEXPRESS
            self.connection_string = f'mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
        else:
            # Para servidor predeterminado
            self.connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
        self.engine = create_engine(self.connection_string)
        print(f"🔌 Conectando a {database}...")

    def preview_csv(self, csv_path="csv_limpios\\drivers_GP.csv"):
        """
        Ver qué contiene el CSV antes de migrar
        """
        try:
            df = pd.read_csv(csv_path)
            print(f"📊 Archivo: {csv_path}")
            print(f"📈 Filas: {len(df)}")
            print(f"📋 Columnas: {list(df.columns)}")
            print("\n🔍 Primeras 5 filas:")
            print(df.head())
            print("\n📝 Tipos de datos:")
            print(df.dtypes)
            print("\n❓ Valores nulos por columna:")
            print(df.isnull().sum())
            return df
        except Exception as e:
            print(f"❌ Error leyendo CSV: {str(e)}")
            return None

    def migrate_drivers(self, csv_path="csv_limpios\\drivers_GP.csv", table_name="drivers"):
        """
        Migrar drivers.csv a SQL Server
        """
        try:
            # Leer CSV
            df = pd.read_csv(csv_path)
            print(f"📊 Cargando drivers_data.csv ({len(df)} conductores)")
            
            # Limpiar nombres de columnas
            original_columns = df.columns.tolist()
            df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('-', '_')
            
            print(f"🧹 Columnas limpiadas:")
            for old, new in zip(original_columns, df.columns):
                if old != new:
                    print(f"  '{old}' → '{new}'")
            
            # Mostrar estructura final
            print(f"\n📋 Estructura final:")
            for col in df.columns:
                print(f"  - {col}: {df[col].dtype}")
            
            # Migrar a SQL
            df.to_sql(
                table_name, 
                self.engine, 
                if_exists='replace',  # Reemplaza si existe
                index=False,
                chunksize=500
            )
            
            print(f"✅ Tabla '{table_name}' creada exitosamente")
            print(f"📈 {len(df)} conductores migrados")
            
            return True
            
        except Exception as e:
            print(f"❌ Error en migración: {str(e)}")
            return False

    def verify_migration(self, table_name="drivers"):
        """
        Verificar que la migración fue exitosa
        """
        try:
            # Contar filas
            count_query = f"SELECT COUNT(*) as total FROM {table_name}"
            result = pd.read_sql(count_query, self.engine)
            total_rows = result['total'].iloc[0]
            
            print(f"✅ Verificación completada:")
            print(f"📊 Total de filas en SQL: {total_rows}")
            
            # Mostrar algunas filas
            sample_query = f"SELECT TOP 5 * FROM {table_name}"
            sample_data = pd.read_sql(sample_query, self.engine)
            print(f"\n🔍 Primeras 5 filas en SQL:")
            print(sample_data)
            
            return True
            
        except Exception as e:
            print(f"❌ Error en verificación: {str(e)}")
            return False

    def test_connection(self):
        """
        Probar conexión a SQL Server con múltiples métodos
        """
        # Método 1: SQLAlchemy
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test"))
                print("✅ Conexión SQLAlchemy exitosa")
                return True
        except Exception as e:
            print(f"❌ SQLAlchemy falló: {str(e)}")
        
        # Método 2: pyodbc directo
        try:
            import pyodbc
            server_parts = self.connection_string.split('/')[2].split('?')[0]
            database = self.connection_string.split('/')[3].split('?')[0]
            
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_parts};DATABASE={database};Trusted_Connection=yes;'
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            conn.close()
            print("✅ Conexión pyodbc directa exitosa")
            
            # Recrear engine con configuración que funciona
            self.connection_string = f'mssql+pyodbc://@{server_parts}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
            self.engine = create_engine(self.connection_string)
            return True
            
        except Exception as e:
            print(f"❌ pyodbc directo falló: {str(e)}")
        
        # Método 3: Intentar con diferentes formatos
        server_options = [
            "NIKU\\SQLEXPRESS",
            ".\\SQLEXPRESS", 
            "localhost\\SQLEXPRESS",
            "(local)\\SQLEXPRESS"
        ]
        
        for server in server_options:
            try:
                import pyodbc
                conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=master;Trusted_Connection=yes;'
                conn = pyodbc.connect(conn_str)
                conn.close()
                print(f"✅ Servidor encontrado: {server}")
                
                # Actualizar configuración
                self.connection_string = f'mssql+pyodbc://@{server}/Formula1?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
                self.engine = create_engine(self.connection_string)
                return True
                
            except Exception:
                continue
        
        print("❌ No se pudo establecer conexión con ningún método")
        print("💡 Verifica que SQL Server esté corriendo y tengas permisos")
        return False

# Script principal
if __name__ == "__main__":
    print("🏎️ Migrador de Drivers F1 a SQL Server")
    print("=" * 50)
    
    # Configuración - CAMBIAR SEGÚN TU SETUP
    SERVER = "NIKU\\SQLEXPRESS"  # Tu servidor SQL (escapado)
    DATABASE = "Formula1"  # Tu base de datos - VERIFICAR QUE EXISTA
    CSV_FILE = "csv_limpios\\drivers_GP.csv"  # Ruta a tu CSV
    TABLE_NAME = "GP"  # Nombre de tabla en SQL
    
    # Verificar que el CSV existe
    if not os.path.exists(CSV_FILE):
        print(f"❌ No se encuentra el archivo: {CSV_FILE}")
        print("💡 Asegúrate de que el archivo esté en la carpeta csv_limpios/")
        exit()
    
    # Crear migrador
    migrator = DriversToSQL(SERVER, DATABASE)
    
    # Proceso paso a paso
    print("\n1️⃣ Previsualizando CSV...")
    df_preview = migrator.preview_csv(CSV_FILE)
    
    if df_preview is not None:
        print(f"\n2️⃣ Probando conexión SQL...")
        if migrator.test_connection():
            
            print(f"\n3️⃣ Migrando datos...")
            if migrator.migrate_drivers(CSV_FILE, TABLE_NAME):
                
                print(f"\n4️⃣ Verificando migración...")
                migrator.verify_migration(TABLE_NAME)
                
                print(f"\n🎉 ¡Migración completada!")
                print(f"📋 Puedes consultar tus datos con: SELECT * FROM {TABLE_NAME}")
            
        else:
            print("❌ No se pudo conectar a SQL Server")
    
    print("\n" + "=" * 50)