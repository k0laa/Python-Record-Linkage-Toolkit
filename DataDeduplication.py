import recordlinkage
from recordlinkage.datasets import load_febrl1

# Veri kümesini yükle
dfA = load_febrl1()

# 1. İndeksleme - aynı 'given_name' değerine sahip kayıtları grupla
indexer = recordlinkage.Index()
indexer.block(left_on='given_name')
candidate_links = indexer.index(dfA)

# 2. Karşılaştırma - çeşitli alanlarda karşılaştırma yap
compare_cl = recordlinkage.Compare()
compare_cl.exact('given_name', 'given_name', label='given_name')
compare_cl.string('surname', 'surname', method='jarowinkler', threshold=0.85, label='surname')
compare_cl.exact('date_of_birth', 'date_of_birth', label='date_of_birth')
compare_cl.exact('suburb', 'suburb', label='suburb')
compare_cl.exact('state', 'state', label='state')
compare_cl.string('address_1', 'address_1', threshold=0.85, label='address_1')

features = compare_cl.compute(candidate_links, dfA)

# 3. Sınıflandırma - eşik üzerinde olan kayıt çiftlerini eşleşme olarak al
matches = features[features.sum(axis=1) > 3]

print(f"Eşleşen kayıt sayısı: {len(matches)}")