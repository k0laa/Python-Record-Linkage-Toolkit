import pandas as pd

class DataLoader:
    def __init__(self, file_path_a, file_path_b):
        self.file_path_a = file_path_a
        self.file_path_b = file_path_b

    def load_data(self):
        try:
            df_a = pd.read_csv(self.file_path_a, index_col='id')
            df_b = pd.read_csv(self.file_path_b, index_col='id')
            return df_a, df_b
        except Exception as e:
            print(f"Veriler yüklenemedi: {e}")
            return None, None
