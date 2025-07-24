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
        self._validate_config()
        
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
    
    def get_results_database_path(self):
        output_config = self.get_output_config()
        db_name = output_config.get('results_database_name', 'results.db')
        return f"../data/{db_name}"
    
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
        
        # recordlinkage konfigürasyonu
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
        
        print("=" * 50)

# Test fonksiyonu
if __name__ == "__main__":
    print("Config Reader Test Starting...")
    
    # Test config oluştur
    test_config = {
        'project_name': 'Test Project',
        'description': 'Test açıklaması',
        'source_database': {
            'path': 'test_source.db',
            'table': 'customers',
            'columns': {
                'id': 'customer_id',
                'name': 'full_name',
                'email': 'email_address'
            }
        },
        'target_database': {
            'path': 'test_target.db',
            'table': 'clients',
            'columns': {
                'id': 'client_id',
                'name': 'client_name',
                'email': 'email'
            }
        },
        'recordlinkage_config': {
            'indexing': {
                'method': 'block',
                'key': 'name'
            },
            'comparison': [
                {'field': 'email', 'method': 'exact'},
                {'field': 'name', 'method': 'string', 'algorithm': 'jarowinkler'}
            ],
            'classification': {
                'method': 'threshold',
                'threshold': 0.7
            }
        }
    }
    
    test_file = 'test_config.yaml'
    with open(test_file, 'w', encoding='utf-8') as f:
        yaml.dump(test_config, f, default_flow_style=False, allow_unicode=True)
    
    try:
        config_reader = ConfigReader(test_file)
        config_reader.print_summary()
        
        print("\nConfig Reader Test Successful")
        
    except Exception as e:
        print(f"Test ERROR: {e}")
    
    finally:
        if os.path.exists(test_file):
            os.remove(test_file) 