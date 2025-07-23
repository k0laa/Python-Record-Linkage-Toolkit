class ReportGenerator:
    @staticmethod
    def generate_report(matches, df_a, df_b):
        if matches.empty:
            print("Hiç eşleşme bulunamadı.")
            return

        print("\nEŞLEŞMELERİN DETAYLI RAPORU:\n")

        for idx_a, idx_b in matches.index:
            record_a = df_a.loc[idx_a]
            record_b = df_b.loc[idx_b]

            print(f"EŞLEŞME: A[{idx_a}] ↔ B[{idx_b}]")
            print(f" - A: {record_a.to_dict()}")
            print(f" - B: {record_b.to_dict()}")
            print("-" * 50)
