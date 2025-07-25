import sqlite3
import pandas as pd
import os
from typing import Optional, Dict, List


class DatabaseManager:
    def __init__(self):
        self.source_connection = None
        self.target_connection = None
        self.results_connection = None
        
        # Çoklu database sistemi için
        self.database_connections = {}  # name -> connection mapping

        print("Database magnager started")

    def connect_databases(self, source_config, target_config, results_db_path=None):
        print("Connecting to databases...")

        # Source database
        source_path = source_config['path']
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Source database not found: {source_path}")

        self.source_connection = sqlite3.connect(source_path)
        self.source_connection.row_factory = sqlite3.Row
        print(f"Source database connected: {source_path}")

        # Target database
        target_path = target_config['path']
        if not os.path.exists(target_path):
            raise FileNotFoundError(f"Target database not found: {target_path}")

        self.target_connection = sqlite3.connect(target_path)
        self.target_connection.row_factory = sqlite3.Row
        print(f"Target database connected: {target_path}")

        # Results database (otomatik oluştur veya var olanı kullan)
        results_path = "../data/results.db"
        os.makedirs(os.path.dirname(results_path), exist_ok=True)

        # Güvenli bağlantı - dosya var olsa bile hata vermez
        try:
            self.results_connection = sqlite3.connect(results_path)
            self.results_connection.row_factory = sqlite3.Row
            if os.path.exists(results_path):
                print(f"Results database (existing): {results_path}")
            else:
                print(f"Results database (new): {results_path}")
        except Exception as e:
            print(f"Warning: Results database connection issue: {e}")
            # Yine de devam et
            self.results_connection = sqlite3.connect(results_path)
            self.results_connection.row_factory = sqlite3.Row
            print(f"Results database connected with warning: {results_path}")

    def connect_multi_databases(self, databases_config: List[dict], results_db_path: str):
        print(f"Connecting to {len(databases_config)} databases...")
        
        # Mevcut bağlantıları temizle
        self.disconnect_all()
        
        # Her database için bağlantı kur
        for db_config in databases_config:
            db_name = db_config['name']
            db_path = db_config['path']
            
            if not os.path.exists(db_path):
                raise FileNotFoundError(f"Database not found: {db_path} (for {db_name})")
            
            connection = sqlite3.connect(db_path)
            connection.row_factory = sqlite3.Row
            self.database_connections[db_name] = connection
            
            print(f"Connected: {db_name} -> {db_path}")
        
        # Results database bağlantısı (güvenli)
        os.makedirs(os.path.dirname(results_db_path), exist_ok=True)
        
        # Dosya zaten var olup olmadığını kontrol et
        file_exists = os.path.exists(results_db_path)
        
        try:
            self.results_connection = sqlite3.connect(results_db_path)
            self.results_connection.row_factory = sqlite3.Row
            
            if file_exists:
                print(f"Results database (existing): {results_db_path}")
            else:
                print(f"Results database (new): {results_db_path}")
                
        except Exception as e:
            print(f"Warning: Results database connection issue: {e}")
            # Yine de devam et
            self.results_connection = sqlite3.connect(results_db_path)
            self.results_connection.row_factory = sqlite3.Row
            print(f"Results database connected with warning: {results_db_path}")
    
    def get_database_connection(self, db_name: str) -> sqlite3.Connection:
        if db_name not in self.database_connections:
            raise ValueError(f"Database connection not found: {db_name}")
        return self.database_connections[db_name]
    
    def validate_multi_database_schemas(self, databases_config: List[dict]):
        print("Validating database schemas...")
        
        for db_config in databases_config:
            db_name = db_config['name']
            connection = self.get_database_connection(db_name)
            
            try:
                table_info = self.validate_table_schema(db_config, connection)
                print(f"{db_name}: {table_info['row_count']} records")
            except Exception as e:
                print(f"{db_name}: {e}")
                raise
    
    def load_data_from_database_by_name(self, db_name: str, db_config: dict, limit: Optional[int] = None) -> pd.DataFrame:
        connection = self.get_database_connection(db_name)
        return self.load_data_from_database(db_config, connection, limit)
    
    def get_all_database_data(self, databases_config: List[dict], limit: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        print("Loading data from all databases...")
        
        data_dict = {}
        for db_config in databases_config:
            db_name = db_config['name']
            df = self.load_data_from_database_by_name(db_name, db_config, limit)
            data_dict[db_name] = df
            print(f"✅ {db_name}: {len(df)} records loaded")
        
        return data_dict

    def disconnect_all(self):
        # Klasik bağlantılar
        if self.source_connection:
            self.source_connection.close()
            self.source_connection = None

        if self.target_connection:
            self.target_connection.close()
            self.target_connection = None

        if self.results_connection:
            self.results_connection.close()
            self.results_connection = None
        
        # Çoklu database bağlantıları
        for name, connection in self.database_connections.items():
            if connection:
                connection.close()
        
        self.database_connections.clear()

        print("All database connections closed.")

    def get_table_info(self, connection: sqlite3.Connection, table_name: str):
        cursor = connection.cursor()

        try:
            # Tablo var mı kontrol et
            cursor.execute("""
                           SELECT name
                           FROM sqlite_master
                           WHERE type = 'table'
                             AND name = ?
                           """, (table_name,))

            if not cursor.fetchone():
                raise ValueError(f"Table not found: {table_name}")

            # Kolon bilgilerini al
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            # Kayıt sayısını al
            cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            count = cursor.fetchone()['count']

            column_info = {}
            for col in columns:
                column_info[col['name']] = {'type': col['type'], 'nullable': not col['notnull'], 'primary_key': bool(col['pk'])}

            return {'table_name': table_name, 'columns': column_info, 'row_count': count}

        except Exception as e:
            raise Exception(f"table info not retrieved: {e}")

    def validate_table_schema(self, db_config, connection: sqlite3.Connection):
        table_name = db_config['table']
        expected_columns = db_config['columns']

        print(f"Table schema is being checked: {table_name}")

        # Tablo bilgilerini al
        table_info = self.get_table_info(connection, table_name)
        actual_columns = table_info['columns']

        # Kolonları kontrol et
        missing_columns = []
        for logical_name, physical_name in expected_columns.items():
            if physical_name not in actual_columns:
                missing_columns.append(physical_name)

        if missing_columns:
            print(f"Warn: Missing columns: {missing_columns}")
        else:
            print(f"Schema is valid: {len(expected_columns)} columns found")

        return table_info

    def load_data_from_database(self, db_config, connection: sqlite3.Connection, limit: Optional[int] = None):
        table_name = db_config['table']
        columns_mapping = db_config['columns']

        print(f"Data is being load: {table_name}")

        # SQL sorgusu oluştur
        physical_columns = list(columns_mapping.values())
        columns_sql = ', '.join(physical_columns)

        query = f"SELECT {columns_sql} FROM {table_name}"

        if limit:
            query += f" LIMIT {limit}"

        try:
            # Veriyi yükle
            df = pd.read_sql_query(query, connection)

            rename_mapping = {v: k for k, v in columns_mapping.items()}
            df = df.rename(columns=rename_mapping)

            print(f"{len(df)} record loaded")
            return df

        except Exception as e:
            raise Exception(f"Data lod ERROR: {e}")

    def get_source_data(self, source_config, limit: Optional[int] = None):
        if not self.source_connection:
            raise ValueError("Source database connection invalid")

        return self.load_data_from_database(source_config, self.source_connection, limit)

    def get_target_data(self, target_config, limit: Optional[int] = None):
        if not self.target_connection:
            raise ValueError("Target database connection invalid")

        return self.load_data_from_database(target_config, self.target_connection, limit)

    def save_results(self, results_df, table_name: str = "match_results"):
        if not self.results_connection:
            raise ValueError("Results database connection invalid")

        print(f"Result are being saved: {table_name}")

        try:
            # Boş DataFrame kontrolü
            if results_df.empty:
                print(f"⚠️ No matches found, creating empty table: {table_name}")
                # Boş tablo oluştur (sadece sütun yapısı ile) - var olan tabloyu değiştir
                results_df.to_sql(table_name, self.results_connection, if_exists='replace', index=False)
                print(f"✅ Empty table created: {table_name}")
                return
            
            # Sonuçları kaydet - var olan tabloyu değiştir
            results_df.to_sql(table_name, self.results_connection, if_exists='replace', index=False)

            print(f"{len(results_df)} results saved.")

        except Exception as e:
            raise Exception(f"Result save ERROR: {e}")
    
    def save_multi_results(self, results_dict: Dict[str, pd.DataFrame], table_prefix: str = "linkage"):
        if not self.results_connection:
            raise ValueError("Results database connection invalid")
        
        print(f"Saving {len(results_dict)} result tables...")
        
        saved_tables = {}
        
        try:
            for comparison_name, results_df in results_dict.items():
                # Boş DataFrame kontrolü
                if results_df.empty:
                    print(f"{comparison_name}: No matches found, skipping table creation")
                    saved_tables[comparison_name] = f"SKIPPED_EMPTY_{comparison_name}"
                    continue
                
                # Tablo adını oluştur: prefix_comparison_name
                table_name = f"{table_prefix}_{comparison_name}".replace('-', '_').replace(' ', '_')
                
                # Sonuçları kaydet - var olan tabloyu değiştir
                results_df.to_sql(table_name, self.results_connection, if_exists='replace', index=False)
                
                saved_tables[comparison_name] = table_name
                print(f"{comparison_name}: {len(results_df)} results -> {table_name}")
            
            # Eğer hiçbir tablo kaydedilmediyse bilgi ver
            if not any(not name.startswith("SKIPPED_EMPTY_") for name in saved_tables.values()):
                print("No matches found in any comparison, no tables created")
            
            return saved_tables
            
        except Exception as e:
            raise Exception(f"Multi-result save ERROR: {e}")

    def export_to_csv(self, results_df: pd.DataFrame, csv_path: str):
        print(f"CSV is being export: {csv_path}")

        try:
            # Directory oluştur (var ise sorun yok)
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)

            # Dosya var mı kontrol et
            file_exists = os.path.exists(csv_path)
            if file_exists:
                print(f"CSV file already exists, will be overwritten: {csv_path}")

            # Boş DataFrame kontrolü
            if results_df.empty:
                print(f"No matches found, creating empty CSV: {csv_path}")
                results_df.to_csv(csv_path, index=False, encoding='utf-8')
                print(f"Empty CSV created: {csv_path}")
                return

            # CSV'ye export et (var olan dosyayı üstüne yaz)
            results_df.to_csv(csv_path, index=False, encoding='utf-8')

            if file_exists:
                print(f"CSV export completed (overwritten): {csv_path}")
            else:
                print(f"CSV export completed (new): {csv_path}")

        except Exception as e:
            raise Exception(f"CSV export ERROR: {e}")
    
    def export_multi_results_to_csv(self, results_dict: Dict[str, pd.DataFrame], base_path: str = "../results"):
        print(f"Exporting {len(results_dict)} result files to CSV...")
        
        exported_files = {}
        
        try:
            # Base directory oluştur (var ise sorun yok)
            os.makedirs(base_path, exist_ok=True)
            
            for comparison_name, results_df in results_dict.items():
                # Boş DataFrame kontrolü
                if results_df.empty:
                    print(f"{comparison_name}: No matches found, skipping CSV export")
                    exported_files[comparison_name] = f"SKIPPED_EMPTY_{comparison_name}.csv"
                    continue
                
                # Dosya adını oluştur
                filename = f"linkage_{comparison_name}.csv".replace('-', '_').replace(' ', '_')
                csv_path = os.path.join(base_path, filename)
                
                # Dosya var mı kontrol et
                file_exists = os.path.exists(csv_path)
                if file_exists:
                    print(f"{comparison_name}: CSV file will be overwritten")
                
                # CSV'ye export et (var olan dosyayı üstüne yaz)
                results_df.to_csv(csv_path, index=False, encoding='utf-8')
                
                exported_files[comparison_name] = csv_path
                if file_exists:
                    print(f"{comparison_name}: {csv_path} (overwritten)")
                else:
                    print(f"{comparison_name}: {csv_path} (new)")
            
            # Eğer hiçbir dosya export edilmediyse bilgi ver
            if not any(not path.startswith("SKIPPED_EMPTY_") for path in exported_files.values()):
                print("No matches found in any comparison, no CSV files created")
            
            return exported_files
            
        except Exception as e:
            raise Exception(f"Multi-CSV export ERROR: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect_all()

