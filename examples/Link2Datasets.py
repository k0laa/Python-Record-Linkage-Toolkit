import recordlinkage
from recordlinkage.datasets import load_febrl4

# Örnek veri setlerini yükle
dfA, dfB = load_febrl4()

# Veri setlerinin sütun adlarını kontrol et
print(dfA.columns)
print(dfB.columns)

# Aday kayıt çiftlerini oluştur
indexer = recordlinkage.Index()
indexer.block("given_name")  # sadece isimleri eşleşenler için bloklama
candidate_links = indexer.index(dfA, dfB)

# Kayıtları karşılaştır
compare_cl = recordlinkage.Compare()
compare_cl.exact("given_name", "given_name", label="given_name")
compare_cl.string("surname", "surname", method="jarowinkler", threshold=0.85, label="surname")
compare_cl.exact("date_of_birth", "date_of_birth", label="date_of_birth")
compare_cl.exact("suburb", "suburb", label="suburb")
compare_cl.exact("state", "state", label="state")
compare_cl.string("address_1", "address_1", threshold=0.85, label="address_1")

# Aday kayıt çiftlerini karşılaştır ve özellikleri çıkar
features = compare_cl.compute(candidate_links, dfA, dfB)

# Basit eşleşme sınıflandırması: Toplam benzerlik skoru 3'ten büyük olanlar eşleşmiş kabul edilir
matches = features[features.sum(axis=1) > 3]
print(f"Eşleşen kayıt sayısı: {len(matches)}")

# Eşleşen kayıtların bir kaçı
for i,idx in enumerate(matches.index):
    if i >= 5:
        break
    print(f"Eşleşen kayıt: "
          f"\n{dfA.loc[idx[0]]} "
          f"\n----------------ile----------------"
          f"\n{dfB.loc[idx[1]]} "
          f"\n Skor: {matches.loc[idx].sum()} "
          f"\n\n\n")

