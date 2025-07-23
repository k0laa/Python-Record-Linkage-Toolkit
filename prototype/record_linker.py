import recordlinkage

class RecordLinker:
    def __init__(self, df_a, df_b):
        self.df_a = df_a
        self.df_b = df_b

    def link_records(self):
        # Bloklama
        indexer = recordlinkage.Index()
        indexer.block('city')
        candidate_links = indexer.index(self.df_a, self.df_b)

        # Karşılaştırma ayarları
        compare = recordlinkage.Compare()

        compare.string('name', 'name',
                       method='jarowinkler', threshold=0.85, label='name')

        compare.string('email', 'email',
                       method='jarowinkler', threshold=0.9, label='email')

        compare.exact('gender', 'gender', label='gender')

        # Aday kayıtlar üzerinde kıyaslama
        features = compare.compute(candidate_links, self.df_a, self.df_b)

        # Skora göre filtreleme eşik:2
        matches = features[features.sum(axis=1) >= 2]

        return matches
