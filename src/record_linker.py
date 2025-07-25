import pandas as pd
import recordlinkage as rl
import time
from typing import Dict
from itertools import combinations


class RecordLinker:
    def __init__(self, config):

        self.config = config

        # recordlinkage bileşenleri
        self.indexer = None
        self.compare_cl = None
        self.classifier = None

        # Sonuçlar
        self.candidate_links = None
        self.features = None
        self.matches = None

        print("Record Linker is being started")
        print(f"Configuration: {config}")

    def setup_indexing(self):
        print("Indexing is being set up...")

        indexing_config = self.config.get('indexing', {})
        method = indexing_config.get('method', 'block')
        key = indexing_config.get('key', None)

        self.indexer = rl.Index()

        if method == 'block':
            if not key:
                raise ValueError("Key required for block method")
            print(f"Block indexing: {key}")
            self.indexer.block(key)

        elif method == 'sortedneighbourhood':
            if not key:
                raise ValueError("Key required for sorted neighborhood")
            window = indexing_config.get('window', 3)
            print(f"Sorted neighbourhood: {key} (window: {window})")
            self.indexer.sortedneighbourhood(key, window=window)

        elif method == 'full':
            print("Full comparison")
            self.indexer.full()

        else:
            raise ValueError(f"Unknown indexing method: {method}")

        print("Indexing ready")
        return self.indexer

    def setup_comparison(self):
        print("Comparison is being set up...")

        comparison_config = self.config.get('comparison', [])

        if not comparison_config:
            raise ValueError("Comparison is empty")

        self.compare_cl = rl.Compare()

        for i, comp in enumerate(comparison_config, 1):
            field = comp['field']
            method = comp['method']

            print(f"  {i}. {field}: {method}")

            if method == 'exact':
                # Exact match
                self.compare_cl.exact(field, field, label=f'{field}_exact')

            elif method == 'string':
                # String similarity
                algorithm = comp.get('algorithm', 'jarowinkler')
                threshold = comp.get('threshold', 0.85)

                print(f"     Algorithm: {algorithm}, Threshold: {threshold}")
                self.compare_cl.string(field, field, method=algorithm, threshold=threshold, label=f'{field}_string')

            elif method == 'numeric':
                # Numeric comparison
                threshold = comp.get('threshold', 1)
                print(f"     Threshold: {threshold}")
                self.compare_cl.numeric(field, field, method='linear', offset=threshold, label=f'{field}_numeric')

            elif method == 'date':
                # Date comparison
                threshold = comp.get('threshold', 365)  # Gün cinsinden
                print(f"     Threshold: {threshold} gün")
                self.compare_cl.date(field, field, swap_month_day=False, offset=threshold, label=f'{field}_date')

            else:
                print(f"Unkown comparison method: {method}")

        print("Compare is ready")
        return self.compare_cl

    def setup_classification(self):
        print("Classification is being set up...")

        classification_config = self.config.get('classification', {})
        method = classification_config.get('method', 'threshold')

        if method == 'threshold':
            threshold = classification_config.get('threshold', 0.7)
            print(f"Threshold-based classification: {threshold}")
            self.classifier = None

        elif method == 'ecm':
            print("ECM (Expectation-Conditional Maximization)")
            self.classifier = rl.ECMClassifier()

        elif method == 'svm':
            print("SVM (Support Vector Machine)")
            self.classifier = rl.SVMClassifier()

        elif method == 'kmeans':
            print("K-Means Clustering")
            self.classifier = rl.KMeansClassifier()

        else:
            raise ValueError(f"Unkown classification method: {method}")

        print("Classification is ready")
        return self.classifier

    def generate_candidate_pairs(self, df_source, df_target):
        if not self.indexer:
            self.setup_indexing()

        print(f"Candidate pairs are being formed...")
        print(f"Source: {len(df_source)} records")
        print(f"Target: {len(df_target)} records")

        start_time = time.time()

        self.candidate_links = self.indexer.index(df_source, df_target)

        elapsed = time.time() - start_time
        total_possible = len(df_source) * len(df_target)
        reduction_ratio = (1 - len(self.candidate_links) / total_possible) * 100

        print(f"{len(self.candidate_links):,} candidate pair created ({elapsed:.2f}s)")
        print(f"Blocking efficiency: %{reduction_ratio:.1f} decrease")

        return self.candidate_links

    def compute_features(self, df_source, df_target):
        if not self.compare_cl:
            self.setup_comparison()

        if self.candidate_links is None:
            self.generate_candidate_pairs(df_source, df_target)

        print(f"Features are being computed...")
        print(f"{len(self.candidate_links):,} pair to be compared")

        start_time = time.time()

        # Özellik karşılaştırmaları
        self.features = self.compare_cl.compute(self.candidate_links, df_source, df_target)

        elapsed = time.time() - start_time

        print(f"Feature compution completed ({elapsed:.2f}s)")
        print(f"Feature matrix size: {self.features.shape}")

        # Özet istatistikler
        print(f"Feature summary:")
        for col in self.features.columns:
            mean_score = self.features[col].mean()
            print(f"   {col}: average {mean_score:.3f}")

        return self.features

    def classify_matches(self):
        if self.features is None:
            raise ValueError("First compute_features() must be run.")

        classification_config = self.config.get('classification', {})
        method = classification_config.get('method', 'threshold')

        print(f"Matches are being sorted: {method}")

        start_time = time.time()

        if method == 'threshold':
            threshold = classification_config.get('threshold', 0.7)

            # Toplam skor hesapla
            scores = self.features.sum(axis=1)
            max_possible_score = len(self.features.columns)

            # Threshold'u uygula
            min_score = threshold * max_possible_score
            self.matches = scores[scores >= min_score]

            print(f"Threshold: {threshold} (min score: {min_score:.1f})")

        else:
            # Machine learning classifiers
            if not self.classifier:
                self.setup_classification()

            print("Classifier is being trained...")
            self.classifier.fit(self.features)

            print("Prediction is being made...")
            match_result = self.classifier.predict(self.features)

            # Boolean series'i matches'e çevir
            if isinstance(match_result, pd.Series):
                self.matches = self.features[match_result].sum(axis=1)
            else:
                self.matches = match_result

        elapsed = time.time() - start_time

        print(f"{len(self.matches)} maches found ({elapsed:.2f}s)")

        if len(self.matches) > 0:
            avg_score = self.matches.mean()
            max_score = self.matches.max()
            min_score = self.matches.min()
            print(f"Skor dağılımı: min={min_score:.3f}, avg={avg_score:.3f}, max={max_score:.3f}")

        return self.matches

    def format_results(self, df_source, df_target):
        if self.matches is None:
            print("Not yet maches")
            return pd.DataFrame()

        print("Results are being formatted...")

        results = []

        for (idx_source, idx_target), total_score in self.matches.items():

            # Source ve target kayıtları
            source_record = df_source.loc[idx_source]
            target_record = df_target.loc[idx_target]

            # Detaylı özellik skorları
            if self.features is not None:
                feature_scores = self.features.loc[(idx_source, idx_target)]
            else:
                feature_scores = pd.Series()

            # Sonuç kaydı oluştur
            result = {'source_id': source_record.get('id', idx_source), 'target_id': target_record.get('id', idx_target), 'total_score': float(total_score), 'max_possible_score': len(self.features.columns) if self.features is not None else 1,
                      'score_ratio': float(total_score) / len(self.features.columns) if self.features is not None else total_score, }

            # Source verileri
            for col in df_source.columns:
                result[f'source_{col}'] = source_record.get(col, '')

            # Target verileri
            for col in df_target.columns:
                result[f'target_{col}'] = target_record.get(col, '')

            # Özellik skorları
            if not feature_scores.empty:
                for feature_name, score in feature_scores.items():
                    result[f'feature_{feature_name}'] = float(score)

            # Match quality assessment
            result['match_quality'] = self._assess_match_quality(result['score_ratio'])
            result['confidence'] = self._assess_confidence(feature_scores)

            results.append(result)

        results_df = pd.DataFrame(results)

        # Skor'a göre sırala
        if not results_df.empty:
            results_df = results_df.sort_values('total_score', ascending=False)

        print(f"{len(results_df)} result formatted")
        return results_df

    def _assess_match_quality(self, score_ratio: float):
        if score_ratio >= 0.9:
            return 'EXCELLENT'
        elif score_ratio >= 0.8:
            return 'GOOD'
        elif score_ratio >= 0.7:
            return 'FAIR'
        elif score_ratio >= 0.5:
            return 'POOR'
        else:
            return 'VERY_POOR'

    def _assess_confidence(self, feature_scores: pd.Series):
        if feature_scores.empty:
            return 'UNKNOWN'

        # Exact match sayısını kontrol et
        exact_matches = sum(1 for col, score in feature_scores.items() if 'exact' in col and score == 1.0)

        if exact_matches >= 2:
            return 'HIGH'
        elif exact_matches == 1:
            return 'MEDIUM'
        elif feature_scores.mean() >= 0.8:
            return 'MEDIUM'
        else:
            return 'LOW'

    def run_full_linkage(self, df_source, df_target):
        print("RECORD LINKAGE STARTING")
        print("=" * 60)

        total_start = time.time()

        try:
            print("\nStep 1: Indexing")
            self.setup_indexing()
            self.generate_candidate_pairs(df_source, df_target)

            print("\nStep 2: Comparison")
            self.setup_comparison()
            self.compute_features(df_source, df_target)

            print("\nStep 3: Classification")
            self.setup_classification()
            self.classify_matches()

            print("\nStep 4: Result Formatting")
            results_df = self.format_results(df_source, df_target)

            total_elapsed = time.time() - total_start

            print("\n" + "=" * 60)
            print("RECORD LINKAGE COMPLETED!")
            print(f"Total duration: {total_elapsed:.2f} seconds")
            print(f"Founded matches: {len(results_df)}")

            if not results_df.empty:
                quality_summary = results_df['match_quality'].value_counts()
                print(f"Quality Distribution: {quality_summary.to_dict()}")

            return results_df

        except Exception as e:
            print(f"\nRecord linkage ERROR: {e}")
            raise

    def get_statistics(self):
        stats = {'total_candidate_pairs': len(self.candidate_links) if self.candidate_links is not None else 0, 'total_matches': len(self.matches) if self.matches is not None else 0,
                 'feature_count': len(self.features.columns) if self.features is not None else 0, 'config': self.config}

        # Blocking efficiency
        if hasattr(self, '_total_possible_pairs'):
            stats['blocking_efficiency'] = (1 - stats['total_candidate_pairs'] / self._total_possible_pairs) * 100

        return stats

    def run_deduplication(self, df_data, data_name: str = "data"):
        print(f"DEDUPLICATION STARTING for {data_name}")
        print("=" * 60)

        total_start = time.time()

        try:
            print("\nStep 1: Indexing for Deduplication")
            self.setup_indexing()
            self.generate_candidate_pairs_dedup(df_data)

            print("\nStep 2: Comparison")
            self.setup_comparison()
            self.compute_features_dedup(df_data)

            print("\nStep 3: Classification")
            self.setup_classification()
            self.classify_matches()

            print("\nStep 4: Result Formatting")
            results_df = self.format_results_dedup(df_data, data_name)

            total_elapsed = time.time() - total_start

            print("\n" + "=" * 60)
            print("DEDUPLICATION COMPLETED!")
            print(f"Total duration: {total_elapsed:.2f} seconds")
            print(f"Founded duplicates: {len(results_df)}")

            if not results_df.empty:
                quality_summary = results_df['match_quality'].value_counts()
                print(f"Quality Distribution: {quality_summary.to_dict()}")

            return results_df

        except Exception as e:
            print(f"\nDeduplication ERROR: {e}")
            raise

    def generate_candidate_pairs_dedup(self, df_data):
        print("Generating candidate pairs for deduplication...")

        if not self.indexer:
            raise ValueError("Indexer not set up")

        start_time = time.time()

        self.candidate_links = self.indexer.index(df_data)

        elapsed = time.time() - start_time
        total_possible = len(df_data) * (len(df_data) - 1) // 2  # n*(n-1)/2

        print(f"Candidate pairs generated: {len(self.candidate_links)}")
        print(f"Total possible pairs: {total_possible}")
        print(f"Blocking reduction: %{((total_possible - len(self.candidate_links)) / total_possible * 100):.1f}")
        print(f"Generation time: {elapsed:.2f} seconds")

        self._total_possible_pairs = total_possible
        return self.candidate_links

    def compute_features_dedup(self, df_data):
        print("Computing features for deduplication...")

        if not self.compare_cl:
            raise ValueError("Comparison not set up")

        if self.candidate_links is None or len(self.candidate_links) == 0:
            print("No candidate pairs to compare")
            self.features = pd.DataFrame()
            return self.features

        start_time = time.time()

        self.features = self.compare_cl.compute(self.candidate_links, df_data)

        elapsed = time.time() - start_time

        print(f"Features computed: {len(self.features)} pairs, {len(self.features.columns)} features")
        print(f"Computation time: {elapsed:.2f} seconds")

        return self.features

    def format_results_dedup(self, df_data, data_name: str):
        print("Formatting deduplication results...")

        if self.matches is None or len(self.matches) == 0:
            print("No matches found")
            return pd.DataFrame()

        results = []

        for (idx1, idx2) in self.matches.index:
            record1 = df_data.loc[idx1]
            record2 = df_data.loc[idx2]

            feature_row = self.features.loc[(idx1, idx2)]
            total_score = feature_row.sum()

            max_score = len(feature_row)
            confidence_score = total_score / max_score if max_score > 0 else 0

            if confidence_score >= 0.9:
                confidence = 'HIGH'
            elif confidence_score >= 0.7:
                confidence = 'MEDIUM'
            else:
                confidence = 'LOW'

            result = {
                f'{data_name}_id_1': idx1,
                f'{data_name}_id_2': idx2,
                'total_score': total_score,
                'confidence': confidence
            }

            # İlk kayıt bilgileri (source olarak)
            for col in df_data.columns:
                result[f'{data_name}_1_{col}'] = record1[col]

            # İkinci kayıt bilgileri (target olarak)
            for col in df_data.columns:
                result[f'{data_name}_2_{col}'] = record2[col]

            # Feature skorları
            for feature_name, score in feature_row.items():
                result[f'feature_{feature_name}'] = score

            results.append(result)

        results_df = pd.DataFrame(results)

        if not results_df.empty:
            # Skorlara göre sırala
            results_df = results_df.sort_values(['total_score', 'match_quality'], ascending=[False, False])

        print(f"Results formatted: {len(results_df)} matches")
        return results_df

    def run_multi_database_linkage(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        print(f"MULTI-DATABASE LINKAGE STARTING ({len(data_dict)} databases)")
        print("=" * 60)

        db_names = list(data_dict.keys())
        all_results = {}

        # 1. Tek database deduplikasyonu
        if len(db_names) == 1:
            db_name = db_names[0]
            print(f"\nRunning deduplication for single database: {db_name}")
            results = self.run_deduplication(data_dict[db_name], db_name)
            all_results[f"{db_name}_dedup"] = results
            return all_results

        # 2. İkili karşılaştırmalar
        print(f"\nRunning pairwise comparisons...")
        for db1, db2 in combinations(db_names, 2):
            comparison_name = f"{db1}_{db2}"
            print(f"\n🔗 Comparing: {db1} ↔ {db2}")
            
            try:
                results = self.run_full_linkage(data_dict[db1], data_dict[db2])
                # Sonuç sütun isimlerini güncelle
                results = self._update_result_column_names(results, db1, db2)
                all_results[comparison_name] = results
                print(f"{comparison_name}: {len(results)} matches found")
            except Exception as e:
                print(f"{comparison_name}: {e}")
                all_results[comparison_name] = pd.DataFrame()

        # 3. Üçlü ve daha fazla karşılaştırmalar
        if len(db_names) >= 3:
            print(f"\nMulti-way comparisons (3+ databases) will be added in future versions...")
            #  Üçlü karşılaştırma mantığı eklenebilir

        print(f"\nTotal comparisons completed: {len(all_results)}")
        return all_results

    def _update_result_column_names(self, results_df: pd.DataFrame, db1_name: str, db2_name: str) -> pd.DataFrame:
        if results_df.empty:
            return results_df

        # Sütun ismlerini güncelle
        column_mapping = {}
        
        for col in results_df.columns:
            if col.startswith('source_'):
                new_col = col.replace('source_', f'{db1_name}_')
                column_mapping[col] = new_col
            elif col.startswith('target_'):
                new_col = col.replace('target_', f'{db2_name}_')
                column_mapping[col] = new_col
        
        if column_mapping:
            results_df = results_df.rename(columns=column_mapping)
        
        return results_df
