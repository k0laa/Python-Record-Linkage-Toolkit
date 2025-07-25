import sys
import os
from typing import Optional, Dict
import time
from datetime import datetime

from config_reader import ConfigReader
from database_manager import DatabaseManager
from record_linker import RecordLinker
import pandas as pd


class LinkageCoordinator:

    def __init__(self, config_path: str):
        print(f"Config file: {config_path}")

        self.config_reader = ConfigReader(config_path)
        self.db_manager = DatabaseManager()
        self.record_linker = None

        # Sistem tipini belirle
        self.is_multi_database = self.config_reader.is_multi_database_config()
        
        if self.is_multi_database:
            print("Multi-database system initialized")
            self._init_multi_database_config()
        else:
            print("Classic two-database system initialized")
            self._init_classic_config()

        print("Coordinator ready")

    def _init_classic_config(self):
        # Konfigürasyonları al
        self.source_config = self.config_reader.get_source_database()
        self.target_config = self.config_reader.get_target_database()
        self.linkage_config = self.config_reader.get_recordlinkage_config()
        self.output_config = self.config_reader.get_output_config()
        self.results_db_path = self.config_reader.get_results_database_path()

    def _init_multi_database_config(self):
        # Konfigürasyonları al
        self.databases_config = self.config_reader.get_databases()
        self.linkage_config = self.config_reader.get_recordlinkage_config()
        self.output_config = self.config_reader.get_output_config()
        self.results_db_path = self.config_reader.get_results_database_path()

    def validate_setup(self):
        print("Setup is being validated...")

        # Config özeti göster
        self.config_reader.print_summary()

        if self.is_multi_database:
            self._validate_multi_database_setup()
        else:
            self._validate_classic_setup()

    def _validate_classic_setup(self):
        try:
            self.db_manager.connect_databases(self.source_config, self.target_config, self.results_db_path)

            self.db_manager.validate_table_schema(self.source_config, self.db_manager.source_connection)
            self.db_manager.validate_table_schema(self.target_config, self.db_manager.target_connection)

            print("classic setup checks passed successfully")

        except Exception as e:
            print(f"Setup ERROR: {e}")
            raise

    def _validate_multi_database_setup(self):
        try:
            self.db_manager.connect_multi_databases(self.databases_config, self.results_db_path)
            self.db_manager.validate_multi_database_schemas(self.databases_config)

            print("Multi-database setup checks passed successfully")

        except Exception as e:
            print(f"Multi-database setup ERROR: {e}")
            raise

    def load_data(self, limit: Optional[int] = None):
        if self.is_multi_database:
            return self._load_multi_database_data(limit)
        else:
            return self._load_classic_data(limit)

    def _load_classic_data(self, limit: Optional[int] = None):
        print("classic data loading...")

        try:
            # Source data
            source_df = self.db_manager.get_source_data(self.source_config, limit)
            print(f"Source data loaded: {len(source_df)} records")

            # Target data
            target_df = self.db_manager.get_target_data(self.target_config, limit)
            print(f"Target data loaded: {len(target_df)} records")

            return source_df, target_df

        except Exception as e:
            print(f"Data load ERROR: {e}")
            raise

    def _load_multi_database_data(self, limit: Optional[int] = None):
        print("Multi-database data loading...")

        try:
            data_dict = self.db_manager.get_all_database_data(self.databases_config, limit)
            
            total_records = sum(len(df) for df in data_dict.values())
            print(f"Total records loaded from {len(data_dict)} databases: {total_records}")
            
            return data_dict

        except Exception as e:
            print(f"Multi-database data load ERROR: {e}")
            raise

    def run_record_linkage(self, source_df, target_df):
        print("Record Linkage are being started...")

        try:
            self.record_linker = RecordLinker(self.linkage_config)
            results_df = self.record_linker.run_full_linkage(source_df, target_df)
            return results_df

        except Exception as e:
            print(f"Record linkage ERROR: {e}")
            raise

    def run_multi_database_linkage(self, data_dict: Dict[str, pd.DataFrame]):
        print("Multi-database linkage starting...")

        try:
            self.record_linker = RecordLinker(self.linkage_config)
            results_dict = self.record_linker.run_multi_database_linkage(data_dict)
            return results_dict

        except Exception as e:
            print(f"Multi-database linkage ERROR: {e}")
            raise

    def save_results(self, results_df):
        print("Results are being saved...")

        saved_files = {}

        try:
            # Database'e kaydet
            if self.output_config.get('save_to_db', True):
                table_name = self.output_config.get('results_table', 'match_results')
                self.db_manager.save_results(results_df, table_name)
                saved_files['database'] = f"{self.results_db_path} -> {table_name}"

            # CSV'ye export et
            if self.output_config.get('export_csv', True):
                csv_path = self.output_config.get('csv_path', '../results/linkage_results.csv')
                self.db_manager.export_to_csv(results_df, csv_path)
                saved_files['csv'] = csv_path

            print("Results saved successfully")
            return saved_files

        except Exception as e:
            print(f"Results save ERROR: {e}")
            return saved_files

    def save_multi_results(self, results_dict):
        print("Multi-database results are being saved...")

        saved_files = {}

        try:
            # Database'e kaydet
            if self.output_config.get('save_to_db', True):
                table_prefix = self.output_config.get('table_prefix', 'linkage')
                saved_tables = self.db_manager.save_multi_results(results_dict, table_prefix)
                saved_files['database'] = saved_tables

            # CSV'ye export et
            if self.output_config.get('export_csv', True):
                csv_base_path = self.output_config.get('csv_base_path', '../results')
                exported_files = self.db_manager.export_multi_results_to_csv(results_dict, csv_base_path)
                saved_files['csv'] = exported_files

            print("Multi-database results saved successfully")
            return saved_files

        except Exception as e:
            print(f"Multi-database results save ERROR: {e}")
            return saved_files

    def generate_report(self, results_data):
        print("Report is being generated...")

        try:
            project_info = self.config_reader.get_project_info()

            if self.is_multi_database:
                return self._generate_multi_database_report(results_data, project_info)
            else:
                return self._generate_classic_report(results_data, project_info)

        except Exception as e:
            print(f"Report create ERROR: {e}")
            return ""

    def _generate_classic_report(self, results_df, project_info):
        # Rapor içeriği
        report_content = f"""# {project_info['name']} - Record Linkage Raporu
            Oluşturulma Tarihi: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            ## Proje Açıklaması
            {project_info['description']}
            
            ## Veritabanları
            - **Source**: {self.source_config['path']} -> {self.source_config['table']}
            - **Target**: {self.target_config['path']} -> {self.target_config['table']}
            
            ## Sonuç Özeti
            - **Toplam Eşleşme**: {len(results_df)}
            
            """

        if not results_df.empty:
            # Kalite dağılımı
            quality_counts = results_df['match_quality'].value_counts()
            report_content += "\n## Kalite Dağılımı\n"
            for quality, count in quality_counts.items():
                percentage = (count / len(results_df)) * 100
                report_content += f"- **{quality}**: {count} (%{percentage:.1f})\n"

            # Güven dağılımı
            confidence_counts = results_df['confidence'].value_counts()
            report_content += "\n## Güven Dağılımı\n"
            for confidence, count in confidence_counts.items():
                percentage = (count / len(results_df)) * 100
                report_content += f"- **{confidence}**: {count} (%{percentage:.1f})\n"

        # Konfigürasyon detayları
        report_content += self._get_config_details()

        return self._save_report(report_content)

    def _generate_multi_database_report(self, results_dict, project_info):
        # Sadece boş olmayan sonuçları say
        non_empty_results = {k: v for k, v in results_dict.items() if not v.empty}
        total_matches = sum(len(df) for df in non_empty_results.values())
        
        report_content = f"""# {project_info['name']} - Multi-Database Record Linkage Raporu
            Oluşturulma Tarihi: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            ## Proje Açıklaması
            {project_info['description']}
            
            ## Veritabanları ({len(self.databases_config)} database)
            """
        
        for i, db in enumerate(self.databases_config, 1):
            report_content += f"{i}. **{db['name']}**: {db['path']} -> {db['table']}\n"

        report_content += f"""
            ## Genel Özet
            - **Toplam Karşılaştırma**: {len(results_dict)}
            - **Başarılı Karşılaştırma**: {len(non_empty_results)}
            - **Toplam Eşleşme**: {total_matches}
            
            ## Karşılaştırma Detayları
            """

        for comparison_name, results_df in results_dict.items():
            report_content += f"\n### {comparison_name}\n"
            
            if results_df.empty:
                report_content += f"- **Eşleşme Sayısı**: 0 (Eşleşme bulunamadı)\n"
                continue
                
            report_content += f"- **Eşleşme Sayısı**: {len(results_df)}\n"
            
            # Kalite dağılımı
            if 'match_quality' in results_df.columns:
                quality_counts = results_df['match_quality'].value_counts()
                report_content += f"- **Kalite Dağılımı**: {dict(quality_counts)}\n"
            
            # Güven dağılımı
            if 'confidence' in results_df.columns:
                confidence_counts = results_df['confidence'].value_counts()
                report_content += f"- **Güven Dağılımı**: {dict(confidence_counts)}\n"

        # Konfigürasyon detayları
        report_content += self._get_config_details()

        return self._save_report(report_content)

    def _get_config_details(self):
        config_content = "\n## Konfigürasyon Detayları\n"
        config_content += f"- **Indexing**: {self.linkage_config.get('indexing', {})}\n"
        config_content += f"- **Comparison**: {len(self.linkage_config.get('comparison', []))} kural\n"
        config_content += f"- **Classification**: {self.linkage_config.get('classification', {})}\n"
        return config_content

    def _save_report(self, report_content):
        # Results directory oluştur (var ise sorun yok)
        os.makedirs('../results', exist_ok=True)
        
        report_path = f"../results/linkage_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        # Dosya var mı kontrol et (aynı saniyede çalışma durumu)
        file_exists = os.path.exists(report_path)
        if file_exists:
            print(f"Report file already exists, will be overwritten: {report_path}")

        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)

            if file_exists:
                print(f"Report created (overwritten): {report_path}")
            else:
                print(f"Report created (new): {report_path}")
            
            return report_path
            
        except Exception as e:
            print(f"Report save ERROR: {e}")
            return ""

    def run_full_pipeline(self, data_limit: Optional[int] = None):
        print("FULL PIPELINE IS STARTED")
        print("=" * 60)

        pipeline_start = time.time()

        try:
            print("\nStep 1: Setup Checks")
            self.validate_setup()

            print("\nStep 2: Data Loading")
            data = self.load_data(data_limit)

            print("\nStep 3: Record Linkage")
            if self.is_multi_database:
                results = self.run_multi_database_linkage(data)
            else:
                source_df, target_df = data
                results = self.run_record_linkage(source_df, target_df)

            print("\nStep 4: Save Results")
            if self.is_multi_database:
                saved_files = self.save_multi_results(results)
            else:
                saved_files = self.save_results(results)

            print("\nStep 5: Generate Report")
            report_path = self.generate_report(results)

            # Pipeline sonuçları
            pipeline_elapsed = time.time() - pipeline_start

            if self.is_multi_database:
                total_matches = sum(len(df) for df in results.values() if not df.empty) if isinstance(results, dict) else 0
                successful_comparisons = len([k for k, v in results.items() if not v.empty]) if isinstance(results, dict) else 0
                
                print("\n" + "=" * 60)
                print("MULTI-DATABASE PIPELINE COMPLETED SUCCESSFULLY")
                print(f"Total Duration: {pipeline_elapsed:.2f} seconds")
                print(f"Total comparisons: {len(results)}")
                print(f"Successful comparisons: {successful_comparisons}")
                print(f"Total matches found: {total_matches}")
            else:
                print("\n" + "=" * 60)
                print("PIPELINE COMPLETED SUCCESSFULLY")
                print(f"Total Duration: {pipeline_elapsed:.2f} seconds")
                print(f"Matches found: {len(results)}")

            print(f"Report: {report_path}")
            print(f"Saved files: {saved_files}")

            return {
                'success': True, 
                'execution_time': pipeline_elapsed, 
                'results': results, 
                'report_path': report_path, 
                'saved_files': saved_files,
                'is_multi_database': self.is_multi_database
            }

        except Exception as e:
            print(f"\n Pipeline ERROR: {e}")
            return {'success': False, 'error': str(e)}

        finally:
            self.db_manager.disconnect_all()
            print("The connections are closed")


def main(config: str, limit: Optional[int] = None, debug: bool = False):
    # Debug mode ayarları
    if debug:
        print("Debug mode is ON")
        print(f"Arguments: 'config': '{config}', 'limit': {limit}, 'debug': {debug}")

    try:
        coordinator = LinkageCoordinator(config)

        results = coordinator.run_full_pipeline(data_limit=limit)

        if results['success']:
            print("\nProgram completed successfully")

            if debug:
                print(f"Debug - Result details: {results}")

            sys.exit(0)
        else:
            print(f"\nProgram ERROR: {results.get('error', 'unknown error')}")
            sys.exit(1)

    except Exception as e:
        print(f"\nUnexpected error: {e}")

        if debug:
            import traceback
            traceback.print_exc()

        sys.exit(1)


if __name__ == "__main__":
    config_path = '../config/templates/multi_db_3_databases.yaml'
    data_limit = None
    debug_mode = False

    main(config_path, data_limit, debug_mode)
