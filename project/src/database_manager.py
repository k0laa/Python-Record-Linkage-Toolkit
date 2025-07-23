import sqlite3
import pandas as pd
import os
from typing import Optional


class DatabaseManager:
    def __init__(self):
        self.source_connection = None
        self.target_connection = None
        self.results_connection = None

        print("Database magnager started")

    def connect_databases(self, source_config, target_config):
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

        # Results database (otomatik oluştur)
        results_path = "../data/results.db"
        os.makedirs(os.path.dirname(results_path), exist_ok=True)

        self.results_connection = sqlite3.connect(results_path)
        self.results_connection.row_factory = sqlite3.Row
        print(f"✅ Results database: {results_path}")

    def disconnect_all(self):
        if self.source_connection:
            self.source_connection.close()
            self.source_connection = None

        if self.target_connection:
            self.target_connection.close()
            self.target_connection = None

        if self.results_connection:
            self.results_connection.close()
            self.results_connection = None

        print("All database connections .")

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
            # Sonuçları kaydet
            results_df.to_sql(table_name, self.results_connection, if_exists='replace', index=False)

            print(f"{len(results_df)} results saved.")

        except Exception as e:
            raise Exception(f"Result save ERROR: {e}")

    def export_to_csv(self, results_df: pd.DataFrame, csv_path: str):
        print(f"CSV is being export: {csv_path}")

        try:
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)

            results_df.to_csv(csv_path, index=False, encoding='utf-8')

            print(f"CSV export completed: {csv_path}")

        except Exception as e:
            raise Exception(f"CSV export ERROR: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect_all()

