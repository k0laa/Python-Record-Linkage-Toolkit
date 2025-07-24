# SQLite Record Linkage

## Proje Açıklaması

Bu proje, 2 farklı SQLite veritabanı arasında recordlinkage toolkit kullanarak kayıt eşleştirmesi bir sistemdir. Sadece YAML konfigürasyon dosyası yazarak herhangi iki veritabanını karşılaştırabilirsiniz!

## Proje Yapısı

```
project/
├── src/                         
│   ├── config_reader.py            # YAML okuyucu
│   ├── database_manager.py         # SQLite yönetimi
│   ├── record_linker.py            # recordlinkage engine
│   └── main.py                     # Ana çalışma dosyası
├── config/                      # Konfigürasyon örnekleri
│   ├── customers_example.yaml      
│   └── template.yaml               # Boş şablon
├── data/                        # SQLite veritabanları
│   ├── crm.db                    
└── └── ecommerce.db      
/requirements.txt             
```

## Hızlı Başlangıç

### ️Kurulum

```bash
  # Gerekli paketleri yükleyin
  pip install -r requirements.txt
```

### Record Linkage Çalıştırma

**`main.py` de main fonksiyonunda config dosyasını belirterek çalıştırabilirsiniz.**

```bash
   cd project
   python src/main.py 
```

## Kullanılan Teknolojiler

- **Python 3.8+** - Ana dil
- **recordlinkage** - linkage library
- **SQLite3** -  database
- **PyYAML** - Configuration files
- **Pandas** - Data manipulation

## recordlinkage Toolkit Detayları

### Indexing Methods

```python
# Block indexing
indexer.block('last_name')

# Sorted neighbourhood  
indexer.sortedneighbourhood('last_name', window=3)

# Full comparison (dikkatli kullan!)
indexer.full()
```

### Comparison Functions

```python
# Exact match
compare.exact('email', 'email')

# String similarity
compare.string('name', 'name', method='jarowinkler')

# Numeric comparison
compare.numeric('age', 'age', method='linear')
```

### Classification Methods

```python
# Threshold-based
classifier = recordlinkage.ThresholdClassifier(0.7)

# Expectation-Conditional Maximization  
classifier = recordlinkage.ECMClassifier()

# Support Vector Machine
classifier = recordlinkage.SVMClassifier()
```


### Kaynaklar

- **recordlinkage docs**: https://recordlinkage.readthedocs.io/
- **SQLite tutorial**: https://www.sqlitetutorial.net/
- **YAML guide**: https://yaml.org/spec/

---
