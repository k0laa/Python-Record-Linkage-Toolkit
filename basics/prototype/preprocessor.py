class Preprocessor:
    @staticmethod
    def preprocess(df):
        df = df.copy()

        # önemli değerleri boş olan satırı at
        df.dropna(subset=["name", "email"], inplace=True)

        for column in ["name", "email", "city"]:
            df[column] = df[column].astype(str).str.lower().str.strip()

        return df
