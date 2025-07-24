import sys
import os
from typing import Optional
import time
from datetime import datetime

from config_reader import ConfigReader
from database_manager import DatabaseManager
from record_linker import RecordLinker


class LinkageCoordinator:

    def __init__(self, config_path: str):
        print(f"Config file: {config_path}")

        self.config_reader = ConfigReader(config_path)
        self.db_manager = DatabaseManager()
        self.record_linker = None

        # Konfigürasyonları al
        self.source_config = self.config_reader.get_source_database()
        self.target_config = self.config_reader.get_target_database()
        self.linkage_config = self.config_reader.get_recordlinkage_config()
        self.output_config = self.config_reader.get_output_config()
        self.results_db_path = self.config_reader.get_results_database_path()

        print("Coordinator ready")

    def validate_setup(self):
        print("Setup is being validated...")

        # Config özeti göster
        self.config_reader.print_summary()

        # Database bağlantıları test et
        try:
            self.db_manager.connect_databases(self.source_config, self.target_config, self.results_db_path)

            self.db_manager.validate_table_schema(self.source_config, self.db_manager.source_connection)
            self.db_manager.validate_table_schema(self.target_config, self.db_manager.target_connection)

            print("Setup checks passed successfully")

        except Exception as e:
            print(f"Setup ERROR: {e}")
            raise

    def load_data(self, limit: Optional[int] = None):

        print("Datas are being loaded...")

        try:
            # Source data
            source_df = self.db_manager.get_source_data(self.source_config, limit)
            print(f"Source data loaded: {len(source_df)} records")

            # Target data
            target_df = self.db_manager.get_target_data(self.target_config, limit)
            print(f"Target data loaded: {len(target_df)} records")

            # Veri önizlemesi
            # print("\nDATA PREVIEW :")
            # print("Source (first 3 records):")
            # print(source_df.head(3))
            #
            # print("\nTarget (first 3 records):")
            # print(target_df.head(3))

            return source_df, target_df

        except Exception as e:
            print(f"Data load ERROR: {e}")
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

    def generate_report(self, results_df):
        print("Report is being generated...")

        try:
            project_info = self.config_reader.get_project_info()

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

                # En iyi eşleştirmeler
                report_content += "\n## En İyi Eşleştirmeler (İlk 10)\n"
                top_matches = results_df.head(10)

                for i, (_, match) in enumerate(top_matches.iterrows(), 1):
                    source_name = match.get('source_name', 'N/A')
                    target_name = match.get('target_name', 'N/A')
                    score = match.get('total_score', 0)
                    quality = match.get('match_quality', 'N/A')

                    report_content += f"{i}. **{source_name}** ↔ **{target_name}**\n"
                    report_content += f"   - Skor: {score:.2f}, Kalite: {quality}\n\n"

            # Konfigürasyon detayları
            report_content += "\n## Konfigürasyon Detayları\n"
            report_content += f"- **Indexing**: {self.linkage_config.get('indexing', {})}\n"
            report_content += f"- **Comparison**: {len(self.linkage_config.get('comparison', []))} kural\n"
            report_content += f"- **Classification**: {self.linkage_config.get('classification', {})}\n"

            # Raporu kaydet
            os.makedirs('results', exist_ok=True)
            report_path = f"../results/linkage_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)

            print(f"Reports created: {report_path}")
            return report_path

        except Exception as e:
            print(f"Report crete ERROR: {e}")
            return ""

    def run_full_pipeline(self, data_limit: Optional[int] = None):
        print("FULL PIPELINE IS STARTED")
        print("=" * 60)

        pipeline_start = time.time()

        try:
            print("\nStep 1: Setup Checks")
            self.validate_setup()

            print("\nStep 2: Data Loading")
            source_df, target_df = self.load_data(data_limit)

            print("\nStep 3: Record Linkage")
            results_df = self.run_record_linkage(source_df, target_df)

            print("\nStep 4: Save Results")
            saved_files = self.save_results(results_df)

            print("\nStep 5: Generate Report")
            report_path = self.generate_report(results_df)

            # Pipeline sonuçları
            pipeline_elapsed = time.time() - pipeline_start

            print("\n" + "=" * 60)
            print("PIPELINE COMPLETED SUCCESSFULLY")
            print(f"Total Duration: {pipeline_elapsed:.2f} seconds")
            print(f"Matches found: {len(results_df)}")
            print(f"Report: {report_path}")
            print(f"Saved files: {saved_files}")

            return {'success': True, 'execution_time': pipeline_elapsed, 'total_matches': len(results_df), 'results_df': results_df, 'report_path': report_path, 'saved_files': saved_files}

        except Exception as e:
            print(f"\n Pipline ERROR: {e}")
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
    config_path = '../config/tenzil.yaml'
    data_limit = None
    debug_mode = False

    main(config_path, data_limit, debug_mode)
