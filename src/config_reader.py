import yaml
import os

class ConfigReader:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = {}

        print(f"Config file is being read: {config_path}")
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        self._load_config()
        
        # Yeni çoklu database sistemini kontrol et
        if 'databases' in self.config:
            self._validate_multi_database_config()
            print("Multi-database configuration detected")
        else:
            self._validate_config()
            print("classic two-database configuration detected")
        
        print("Config file read successfully")
    
    def _load_config(self):
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self.config = yaml.safe_load(file)
                
        except yaml.YAMLError as e:
            raise ValueError(f"YAML file is invalid: {e}")
        except Exception as e:
            raise Exception(f"File read ERROR: {e}")
    
    def _validate_config(self):
        print("Configuration is being checked...")
        
        # Zorunlu alanlar
        required_fields = [
            'source_database',
            'target_database', 
            'recordlinkage_config'
        ]
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Required field missing: {field}")
        
        # Source database kontrolleri
        self._validate_database_config('source_database')
        self._validate_database_config('target_database')
        
        # recordlinkage config kontrolleri
        self._validate_recordlinkage_config()
        
        print("Configuration valid")

    def _validate_multi_database_config(self):
        print("Multi-database configuration is being validated...")
        
        # Zorunlu alanlar
        required_fields = [
            'databases',
            'recordlinkage_config'
        ]
        
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Required field missing: {field}")
        
        # Database listesi kontrolleri
        databases = self.config['databases']
        if not isinstance(databases, list) or len(databases) == 0:
            raise ValueError("databases field should be a non-empty list")
        
        # Her database konfigürasyonunu kontrol et
        for i, db_config in enumerate(databases):
            if 'name' not in db_config:
                raise ValueError(f"Database {i}: 'name' field missing")
            
            self._validate_single_database_config(db_config, f"Database {i} ({db_config.get('name', 'unnamed')})")
        
        # Database isimlerinin benzersiz olduğunu kontrol et
        db_names = [db['name'] for db in databases]
        if len(db_names) != len(set(db_names)):
            raise ValueError("Database names must be unique")
        
        # recordlinkage config kontrolleri
        self._validate_recordlinkage_config()
        
        print("Multi-database configuration valid")
    
    def _validate_single_database_config(self, db_config: dict, context: str):
        # Zorunlu alanlar
        required = ['name', 'path', 'table', 'columns']
        for field in required:
            if field not in db_config:
                raise ValueError(f"{context}.{field} missing")
        
        db_path = db_config['path']
        if not os.path.exists(db_path):
            print(f"Warn: Database file not found: {db_path}")
        
        # Columns alt alanları
        columns = db_config['columns']
        if not isinstance(columns, dict) or len(columns) == 0:
            raise ValueError(f"{context}.columns empty or invalid")

    def _validate_database_config(self, db_key: str):
        db_config = self.config[db_key]
        
        # Zorunlu alanlar
        required = ['path', 'table', 'columns']
        for field in required:
            if field not in db_config:
                raise ValueError(f"{db_key}.{field} missing")
        
        db_path = db_config['path']
        if not os.path.exists(db_path):
            print(f"Warn: Database file not found: {db_path}")
        
        # Columns alt alanları
        columns = db_config['columns']
        if not isinstance(columns, dict) or len(columns) == 0:
            raise ValueError(f"{db_key}.columns empty or invalid")
    
    def _validate_recordlinkage_config(self):

        rl_config = self.config['recordlinkage_config']
        
        # Indexing kontrolleri
        if 'indexing' in rl_config:
            indexing = rl_config['indexing']
            valid_methods = ['block', 'sortedneighbourhood', 'full']
            
            if 'method' in indexing:
                if indexing['method'] not in valid_methods:
                    raise ValueError(f"Invalid indexing method: {indexing['method']}")
        
        # Comparison kontrolleri
        if 'comparison' in rl_config:
            comparison = rl_config['comparison']
            if not isinstance(comparison, list):
                raise ValueError("comparison should be a list")
            
            for comp in comparison:
                if 'field' not in comp or 'method' not in comp:
                    raise ValueError("comparison should contain field and method")
        
        # Classification kontrolleri
        if 'classification' in rl_config:
            classification = rl_config['classification']
            valid_methods = ['threshold', 'ecm', 'svm', 'kmeans']
            
            if 'method' in classification:
                if classification['method'] not in valid_methods:
                    raise ValueError(f"Invalid classification method: {classification['method']}")
    
    def is_multi_database_config(self) -> bool:
        return 'databases' in self.config
    
    def get_databases(self) -> list:
        if not self.is_multi_database_config():
            raise ValueError("This is not a multi-database configuration")
        
        return self.config['databases']
    
    def get_database_by_name(self, name: str) -> dict:
        if not self.is_multi_database_config():
            raise ValueError("This is not a multi-database configuration")
        
        for db in self.config['databases']:
            if db['name'] == name:
                return db
        
        raise ValueError(f"Database not found: {name}")
    
    def get_database_names(self) -> list:
        if not self.is_multi_database_config():
            return ['source', 'target']  # Geleneksel sistem için
        
        return [db['name'] for db in self.config['databases']]

    def get_config(self):
        return self.config
    
    def get_source_database(self):
        return self.config['source_database']
    
    def get_target_database(self):
        return self.config['target_database']
    
    def get_recordlinkage_config(self):
        return self.config['recordlinkage_config']
    
    def get_output_config(self):
        return self.config.get('output', {})
    
    def get_project_info(self):
        return {
            'name': self.config.get('project_name', 'Unnamed Project'),
            'description': self.config.get('description', 'No description')
        }
    
    def get_comparison_fields(self):
        rl_config = self.get_recordlinkage_config()
        comparison = rl_config.get('comparison', [])
        
        fields = []
        for comp in comparison:
            if 'field' in comp:
                fields.append(comp['field'])
        
        return fields
    
    def print_summary(self):
        print("\nCONFIGURATION SUMMARY:")
        print("=" * 50)
        
        # Proje bilgileri
        project_info = self.get_project_info()
        print(f"Project: {project_info['name']}")
        print(f"Description: {project_info['description']}")
        
        if self.is_multi_database_config():
            self._print_multi_database_summary()
        else:
            self._print_classic_summary()
    
    def _print_multi_database_summary(self):
        databases = self.get_databases()
        
        print(f"\nDATABASES ({len(databases)} databases):")
        for i, db in enumerate(databases, 1):
            print(f"{i}. {db['name']}: {db['path']} -> {db['table']}")
        
        # Kolon eşleştirmeleri (ilk database'den örnek)
        if databases:
            print(f"\nCOLUMN MAPPINGS (example from {databases[0]['name']}):")
            cols = databases[0]['columns']
            for logical_name, physical_name in cols.items():
                print(f"  {logical_name} -> {physical_name}")
        
        self._print_recordlinkage_summary()
    
    def _print_classic_summary(self):
        # Database bilgileri
        source_db = self.get_source_database()
        target_db = self.get_target_database()
        
        print(f"\nDATABASES:")
        print(f"Source: {source_db['path']} -> {source_db['table']}")
        print(f"Target: {target_db['path']} -> {target_db['table']}")
        
        # Kolon eşleştirmeleri
        print(f"\n🔗 KOLON EŞLEŞTİRMELERİ:")
        print("Source Column -> Target Column")
        
        source_cols = source_db['columns']
        target_cols = target_db['columns']
        
        for logical_name in source_cols:
            if logical_name in target_cols:
                print(f"  {source_cols[logical_name]} -> {target_cols[logical_name]}")
        
        self._print_recordlinkage_summary()
    
    def _print_recordlinkage_summary(self):
        rl_config = self.get_recordlinkage_config()
        print(f"\nRECORDLINKAGE SETTINGS:")
        
        if 'indexing' in rl_config:
            indexing = rl_config['indexing']
            print(f"Indexing: {indexing.get('method', 'default')} (key: {indexing.get('key', 'N/A')})")
        
        if 'comparison' in rl_config:
            print(f"Comparison Rules: {len(rl_config['comparison'])} unit")
            for i, comp in enumerate(rl_config['comparison'], 1):
                method = comp.get('method', 'unknown')
                field = comp.get('field', 'unknown')
                print(f"  {i}. {field}: {method}")
        
        if 'classification' in rl_config:
            classification = rl_config['classification']
            method = classification.get('method', 'threshold')
            threshold = classification.get('threshold', 0.7)
            print(f"Classification: {method} (threshold: {threshold})")

    def get_results_database_path(self):
        output_config = self.get_output_config()
        
        if self.is_multi_database_config():
            # Çoklu database sisteminde sonuç database yolu
            results_path = output_config.get('results_database_path', '../data/multi_linkage_results.db')
        else:
            # Geleneksel sistemde
            results_db_name = output_config.get('results_database_name', 'linkage_results.db')
            results_path = f"../data/{results_db_name}"
        
        # Directory'nin var olduğundan emin ol
        try:
            os.makedirs(os.path.dirname(results_path), exist_ok=True)
        except Exception as e:
            print(f"Warning: Could not create results directory: {e}")
        
        return results_path