# SQLite Record Linkage - Çoklu Database Sistemi

## Proje Açıklaması

Bu proje, **herhangi sayıdaki SQLite veritabanı** arasında recordlinkage toolkit kullanarak kapsamlı kayıt eşleştirmesi yapan gelişmiş bir sistemdir. Artık sadece YAML konfigürasyon dosyası yazarak:

- **1 Database**: Deduplikasyon (tekrarlayan kayıt temizleme)
- **2 Database**: Geleneksel ikili karşılaştırma  
- **3+ Database**: Tüm kombinasyonları otomatik karşılaştırma

## Yeni Özellikler ⭐️⭐️⭐️

### Çoklu Database Desteği
- **N tane database** verildiğinde tüm ikili kombinasyonları otomatik karşılaştırır
- **Tek database** verildiğinde deduplikasyon yapar
- **Sonuçlar** ayrı tablolarda kaydedilir (örn: `crm_ecommerce`, `crm_mobile_app`)
- **Mevcut sistem** bozulmadan korunur - geriye dönük uyumlu

### Akıllı Kombinasyon Mantığı
```
1 DB: customers_dedup
2 DB: crm_ecommerce  
3 DB: crm_ecommerce, crm_mobile, ecommerce_mobile
4 DB: 6 farklı ikili kombinasyon
5 DB: 10 farklı ikili kombinasyon
```

## Proje Yapısı

```
RecordLinkage/
├── basics/                                  # Temel recordlinkage örnekleri
├── src/                          
│   ├── config_reader.py                     # YAML okuyucu (çoklu DB desteği)
│   ├── database_manager.py                  # SQLite yönetimi (çoklu bağlantı)
│   ├── record_linker.py                     # recordlinkage engine (deduplikasyon)
│   └── main.py                              # Ana çalışma dosyası (koordinatör)
├── config/                                  # Konfigürasyon örnekleri 
│   ├──  templates/                          # Çoklu database şablonları
│   │    ├── multi_db_1_database.yaml        # Deduplikasyon örneği
│   │    ├── multi_db_2_databases.yaml       # İkili karşılaştırma
│   │    ├── multi_db_3_databases.yaml       # Üçlü sistem
│   │    ├── multi_db_4_databases.yaml       # Dörtlü sistem
│   │    ├── multi_db_5_databases.yaml       # Beşli sistem
│   │    └── template.yaml                   # Geleneksel örnek
│   └── customers_example.yaml               # Geleneksel şablon
├── data/                                    # SQLite veritabanları
├── results/                                 # Sonuç dosyaları
├── requirements.txt                         # Gerekli Python paketleri
└── README.md                                # Proje açıklaması ve kullanım kılavuzu

```

## Hızlı Başlangıç

### Kurulum

```bash
  # Gerekli paketleri yükleyin
  pip install -r requirements.txt
```

### Kullanım Örnekleri
**`main.py` de main fonksiyonunda config dosyasını belirterek çalıştırabilirsiniz.**
 

```python
# src/main.py
if __name__ == "__main__":
    config_path = "config/multi_db_1_database.yaml"
```
```bash
  python src/main.py 
```


## Konfigürasyon Formatları

### Yeni Çoklu Database Formatı

```yaml
# ÇOKLU VERİTABANI SİSTEMİ
databases:
  - name: "crm"
    path: "../data/crm.db"
    table: "customers"
    columns:
      id: "customer_id"
      name: "full_name"
      email: "email_address"
      # ... diğer alanlar

  - name: "ecommerce"  
    path: "../data/ecommerce.db"
    table: "users"
    columns:
      id: "user_id"
      name: "display_name"
      email: "email"
      # ... diğer alanlar

# Çıktı ayarları
output:
  save_to_db: true
  results_database_path: "../data/multi_linkage_results.db"
  table_prefix: "linkage"
  export_csv: true
  csv_base_path: "../results/multi"
```

### Klasik Format (Hala Destekleniyor)

```yaml
# KAYNAK VERİTABANI
source_database:
  path: "../data/source.db"
  table: "customers"
  # ...

# HEDEF VERİTABANI  
target_database:
  path: "../data/target.db"
  table: "users"
  # ...
```


## Kullanılan Teknolojiler

- **Python 3.8+** - Ana dil
- **recordlinkage** - Linkage library
- **SQLite3** - Database
- **PyYAML** - Configuration files
- **Pandas** - Data manipulation

## Record Linkage Detayları

### Indexing Methods

```python
# Block indexing
indexer.block('email')

# Sorted neighbourhood - Orta hız
indexer.sortedneighbourhood('name', window=5)

# Full comparison - Yavaş ama kapsamlı
indexer.full()
```

### Comparison Functions

```python
# Exact match
compare.exact('email', 'email')

# String similarity
compare.string('name', 'name', method='jarowinkler', threshold=0.85)

# Numeric comparisona
compare.numeric('age', 'age', method='linear')
```

### Classification Methods

```python
# Threshold - Eşik değeri
classifier = rl.ThresholdClassifier(0.7)

# Machine Learning yaklaşımları
classifier = rl.ECMClassifier()  # Expectation-Maximization
classifier = rl.SVMClassifier()  # Support Vector Machine
```



### Karşılaşılabilecek Hatalar
1. **Database bulunamadı**: Path kontrolü yapın
2. **Sütun eşleşmiyor**: Schema validation loglarını kontrol edin  
3. **Çok az eşleşme**: Threshold değerlerini düşürün
4. **Çok yavaş**: Indexing metodunu 'block' yapın

---

### Kaynaklar

- **recordlinkage docs**: https://recordlinkage.readthedocs.io/
- **SQLite tutorial**: https://www.sqlitetutorial.net/
- **YAML guide**: https://yaml.org/spec/



## Lisans

MIT License - Detaylar için LICENSE dosyasına bakın.
