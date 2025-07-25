# Python Record Linkage Toolkit  

## Python Record Linkage Toolkit Nedir?
>Python Record Linkage Toolkit, farklı veri kaynaklarındaki 
> benzer ya da aynı varlıkları temsil eden kayıtları eşleştirmek 
> kayıt bağlantısı yapmak) için kullanılan bir Python kütüphanesidir. 
> Veri temizleme, indeksleme (örneğin blocking), 
> öznitelik karşılaştırma (string benzerliği, tam eşleşme vb.) 
> ve sınıflandırma (lojistik regresyon, EM algoritması gibi) 
> adımlarını içeren tam bir iş akışı sunar. Bu sayede kullanıcılar 
> hem veri setleri arası eşleştirme, hem de veri içi tekilleştirme 
> işlemlerini kolayca gerçekleştirebilir. Kütüphane, özellikle küçük 
> ve orta ölçekli projeler için uygundur ve pandas ile entegre 
> çalışarak esnek ve güçlü bir çözüm sunar.

## Kurulum

Python 3.6+ sürümleriyle uyumludur. Kurmak için:
```bash
pip install recordlinkage
```
Tüm ek özelliklerle birlikte kurmak için:

```bash
pip install recordlinkage['all']
```

## Kullanım

### İki Veri Kümesini Eşleştirme

Aşağıdaki örnek, iki veri kümesini belirli özniteliklere göre eşleştirmek için Python Record Linkage Toolkit'in nasıl kullanılacağını gösterir.

#### Örnek Kod

```python
import recordlinkage
from recordlinkage.datasets import load_febrl4

# Örnek veri setlerini yükle
dfA, dfB = load_febrl4()

# Aday kayıt çiftlerini oluştur
indexer = recordlinkage.Index()
indexer.block("given_name")  # 'given_name' sütununda eşleşenleri al
candidate_links = indexer.index(dfA, dfB)

# Kayıtları karşılaştır
compare_cl = recordlinkage.Compare()
compare_cl.exact("given_name", "given_name", label="given_name")
compare_cl.string("surname", "surname", method="jarowinkler", threshold=0.85, label="surname")
compare_cl.exact("date_of_birth", "date_of_birth", label="date_of_birth")
compare_cl.exact("suburb", "suburb", label="suburb")
compare_cl.exact("state", "state", label="state")
compare_cl.string("address_1", "address_1", threshold=0.85, label="address_1")

features = compare_cl.compute(candidate_links, dfA, dfB)

# Basit eşleşme sınıflandırması: Toplam benzerlik skoru 3'ten büyük olanlar eşleşmiş kabul edilir
matches = features[features.sum(axis=1) > 3]
print(f"Eşleşen kayıt sayısı: {len(matches)}")
```

Bu örnek, bloklama, karşılaştırma ve basit eşikleme yoluyla kayıtları eşleştirme sürecini gösterir.



### Veri Tekilleştirme (Deduplication)

Veri tekilleştirme, aynı varlığa ait tekrar eden kayıtların veri setinden tespit edilip birleştirilmesi işlemidir. Bu süreç özellikle kişisel kayıtlar gibi çoklu veri girişlerinin olduğu durumlarda önemlidir.

#### Süreç Adımları

1. **İndeksleme (Indexing):**  
   Veri kümesindeki kayıt çiftleri oluşturulur. Bu işlem, örneğin aynı isim (`given_name`) değerine sahip kayıtların bir araya getirilmesi gibi bloklama yöntemleriyle yapılır. Böylece karşılaştırılacak kayıt sayısı azaltılır.

2. **Karşılaştırma (Comparison):**  
   Oluşturulan aday kayıt çiftleri, isim, soyadı, doğum tarihi, adres gibi sütunlar üzerinde çeşitli karşılaştırma yöntemleri (kesin eşleşme, metin benzerliği gibi) kullanılarak değerlendirilir.

3. **Sınıflandırma (Classification):**  
   Karşılaştırma sonucu elde edilen skorlar toplanır ve belirlenen eşik değerin üzerinde olan kayıt çiftleri aynı varlık olarak kabul edilir.

4. **Sonuç:**  
   Tekilleştirilen kayıtlar, yinelenen verilerden arındırılmış daha temiz bir veri seti sağlar.

#### Örnek Python Kodu

```python
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
