from data_loader import DataLoader
from preprocessor import Preprocessor
from record_linker import RecordLinker
from report_generator import ReportGenerator

if __name__ == "__main__":
    # Veri Yükle
    loader = DataLoader("data/customers_a.csv", "data/customers_b.csv")
    df_a, df_b = loader.load_data()

    # Ön İşleme
    df_a_clean = Preprocessor.preprocess(df_a)
    df_b_clean = Preprocessor.preprocess(df_b)

    # Eşleştirme
    linker = RecordLinker(df_a_clean, df_b_clean)
    matches = linker.link_records()

    # Raporlama
    ReportGenerator.generate_report(matches, df_a_clean, df_b_clean)
