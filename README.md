# Big Data Praktikum 1-7
# Modul Praktikum Big Data

Dokumentasi dan panduan untuk pengerjaan Modul Praktikum Big Data.

---

## Modul 1: Infrastruktur Big Data (HDFS, MongoDB, Cassandra)

### Bagian 1.1: HDFS

#### Eksperimen Dasar

1.  **Membuat folder di HDFS:**
    ```shell
    hdfs dfs -mkdir /praktikum
    ```

2.  **Upload file `dataset.csv`:**
    ```shell
    hdfs dfs -put dataset.csv /praktikum/
    ```

3.  **Menampilkan isi folder:**
    ```shell
    hdfs dfs -ls /praktikum/
    ```

4.  **Membaca file:**
    ```shell
    hdfs dfs -cat /praktikum/dataset.csv
    ```

#### Latihan: Upload File Besar

Untuk latihan ini, Anda perlu membuat sebuah file besar (>100MB). Karena Anda menggunakan Windows, Anda bisa menggunakan perintah `fsutil` di Command Prompt atau PowerShell untuk membuat file kosong berukuran 100MB.

1.  **Buka Terminal dan navigasi ke folder ini.**

2.  **Buat file 100MB bernama `large_dataset.csv`:**
    ```shell
    fsutil file createnew large_dataset.csv 104857600
    ```
    *(104857600 adalah 100MB dalam bytes)*

3.  **Upload file tersebut ke HDFS:**
    ```shell
    hdfs dfs -put large_dataset.csv /praktikum/
    ```

4.  **Periksa status bloknya** melalui UI HDFS (biasanya di `http://localhost:9870`).

---

### Bagian 1.2: Praktikum MongoDB

#### 1. Instalasi & Menjalankan MongoDB (via Docker)

```shell
docker run -d -p 27017:27017 --name mongo mongo:latest
```

#### 2. Masuk ke Shell MongoDB

```shell
docker exec -it mongo mongosh
```

#### 3. Eksperimen Dasar & Lanjutan (dalam `mongosh`)

```javascript
// Membuat atau menggunakan database 'praktikum'
use praktikum

// Insert satu data
db.mahasiswa.insertOne({ nim: "12345", nama: "Andi", jurusan: "Informatika" })

// Menampilkan semua data
db.mahasiswa.find()

// Insert banyak data
db.mahasiswa.insertMany([
 { nim: "12346", nama: "Budi", jurusan: "Sistem Informasi" },
 { nim: "12347", nama: "Citra", jurusan: "Teknik Komputer" }
])

// Membuat index
db.mahasiswa.createIndex({ nim: 1 })
```

#### Latihan: Nested JSON

```javascript
db.mahasiswa.insertOne({
  nim: "12348",
  nama: "Dina",
  jurusan: "Informatika",
  biodata: {
    alamat: "Jl. Merdeka No. 10",
    kontak: { email: "dina@example.com", telepon: "081234567890" }
  }
})
db.mahasiswa.find({ nim: "12348" })
```

---

### Bagian 1.3: Praktikum Cassandra

#### 1. Instalasi & Menjalankan Cassandra (via Docker)

```shell
docker run --name cassandra -d -p 9042:9042 cassandra:latest
```

#### 2. Masuk ke Shell CQL

```shell
docker exec -it cassandra cqlsh
```

#### 3. Eksperimen Dasar & Lanjutan (dalam `cqlsh`)

```cql
CREATE KEYSPACE praktikum WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE praktikum;
CREATE TABLE mahasiswa (nim text PRIMARY KEY, nama text, jurusan text);
INSERT INTO mahasiswa (nim, nama, jurusan) VALUES ('12345', 'Budi', 'Informatika');
SELECT * FROM mahasiswa;
```

#### Latihan: Cluster Cassandra dengan Docker Compose

Gunakan file `docker-compose.yml` yang telah disediakan.

1.  Jalankan cluster:
    ```shell
    docker-compose up -d
    ```
2.  Periksa status node:
    ```shell
    docker exec -it cassandra-node1 nodetool status
    ```

---
---

## Modul 2: Pemrosesan Data - Word Count

Kasus studi ini membandingkan 3 teknologi untuk tugas Word Count menggunakan file `input.txt`.

### Bagian 2.1: MapReduce (Hadoop Streaming)

Model pemrosesan berbasis disk.

#### 1. Persiapan Data di HDFS

```shell
hdfs dfs -mkdir -p /user/latihan_mr/input
hdfs dfs -put input.txt /user/latihan_mr/input
```

#### 2. Kode Mapper (`mapper.py`)

Skrip ini mengubah setiap baris teks menjadi pasangan `(kata, 1)`. File `mapper.py` sudah dibuatkan untuk Anda. Pastikan file ini executable: `chmod +x mapper.py`.

#### 3. Kode Reducer (`reducer.py`)

Skrip ini menerima output dari mapper, mengelompokkan berdasarkan kata, dan menjumlahkan hitungannya. File `reducer.py` sudah dibuatkan untuk Anda. Pastikan file ini executable: `chmod +x reducer.py`.

#### 4. Eksekusi Job MapReduce

```shell
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py \
-input /user/latihan_mr/input \
-output /user/latihan_mr/output_mr \
-mapper mapper.py \
-reducer reducer.py
```
*Catatan: Periksa hasil di HDFS dengan `hdfs dfs -ls /user/latihan_mr/output_mr` dan `hdfs dfs -cat /user/latihan_mr/output_mr/part-00000`.*

---

### Bagian 2.2: Spark RDD

Model pemrosesan in-memory dengan API fungsional. Jalankan kode ini di dalam `pyspark` shell.

```python
# 1. Muat data ke RDD
lines_rdd = spark.sparkContext.textFile("input.txt")

# 2. Transformasi Word Count
counts_rdd = lines_rdd.flatMap(lambda line: line.lower().split(" ")) \
                      .map(lambda word: (word, 1)) \
                      .reduceByKey(lambda a, b: a + b)

# 3. Aksi untuk melihat hasil
final_counts = counts_rdd.collect()
for word, count in final_counts[:10]:
    print(f"{word}: {count}")
```

---

### Bagian 2.3: Spark DataFrame

Model pemrosesan terstruktur dengan optimasi dari Catalyst Optimizer. Jalankan di `pyspark` shell.

```python
from pyspark.sql.functions import explode, split, col

# 1. Muat data sebagai DataFrame
df = spark.read.text("input.txt")

# 2. Transformasi menggunakan DataFrame API
counts_df = df.select(
    explode(split(col("value"), " ")).alias("word")
).filter(col("word") != "") \
 .groupBy("word").count()

# 3. Aksi untuk menampilkan hasil
counts_df.orderBy(col("count").desc()).show(10)
```

### Analisis & Perbandingan

Modul ini bertujuan agar Anda dapat menganalisis perbedaan dari ketiga pendekatan tersebut:
- **Sintaks & Abstraksi**: Bandingkan kompleksitas kode MapReduce vs RDD vs DataFrame.
- **Kinerja**: Amati perbedaan waktu eksekusi (DataFrame biasanya tercepat karena Catalyst Optimizer).
- **Lazy Evaluation**: Pahami konsep eksekusi yang ditunda di Spark hingga sebuah *action* dipanggil.

---
---

## Modul 3: Integrasi dan Ingesti Data

Modul ini membahas 3 alat untuk memindahkan data ke dalam ekosistem Big Data.

### Bagian 3.1: Apache Sqoop (Transfer Data Relasional)

Sqoop digunakan untuk transfer data antara database relasional (seperti MySQL) dan HDFS.

#### 1. Persiapan Database MySQL

Jalankan perintah SQL berikut di dalam MySQL untuk membuat data sumber.
```sql
CREATE DATABASE company;
USE company;
CREATE TABLE employees (id INT, name VARCHAR(50));
INSERT INTO employees VALUES (1, 'Andi'), (2, 'Budi'), (3, 'Citra');
```

#### 2. Jalankan Sqoop Import

Perintah ini akan menarik data dari tabel `employees` di MySQL ke HDFS.
*Catatan: Pastikan konektor JDBC MySQL sudah ada di folder `lib` Sqoop.*

```shell
./bin/sqoop import \
--connect jdbc:mysql://localhost/company \
--username root \
--password 'password_anda' \
--table employees \
--target-dir /user/hadoop/employees \
-m 1
```

#### 3. Verifikasi di HDFS

```shell
hdfs dfs -ls /user/hadoop/employees
hdfs dfs -cat /user/hadoop/employees/part-m-00000
```

---

### Bagian 3.2: Apache Flume (Ingesti Data Streaming)

Flume digunakan untuk mengumpulkan data log atau event secara streaming.

#### 1. Konfigurasi Flume Agent

Buat file `netcat-logger.conf` di dalam direktori `conf` Flume. File ini sudah saya buatkan untuk Anda di folder `BigData/conf`. Isinya adalah sebagai berikut:

```properties
# Agent components
a1.sources = r1
a1.sinks = k1
a1.channels = c1
# Configure the source
a1.sources.r1.type = netcat
a1.sources.r1.bind = localhost
a1.sources.r1.port = 44444
# Configure the sink
a1.sinks.k1.type = logger
# Configure the channel
a1.channels.c1.type = memory
# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```

#### 2. Jalankan Flume Agent

Buka terminal dan jalankan perintah dari direktori instalasi Flume:
```shell
./bin/flume-ng agent --conf conf --conf-file conf/netcat-logger.conf --name a1 -Dflume.root.logger=INFO,console
```

#### 3. Kirim Data untuk Uji Coba

Buka terminal kedua dan gunakan `telnet` atau `nc` untuk mengirim data ke Flume.
```shell
telnet localhost 44444
```
Ketik pesan apa saja dan tekan Enter. Anda akan melihat pesan tersebut muncul di log pada terminal tempat Flume berjalan.

---

### Bagian 3.3: Apache Kafka (Platform Pesan Terdistribusi)

Kafka bertindak sebagai perantara untuk sistem producer-consumer.

#### 1. Jalankan Zookeeper & Kafka Server (masing-masing di terminal terpisah)

```shell
# Terminal 1: Jalankan Zookeeper
./bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Jalankan Kafka Broker
./bin/kafka-server-start.sh config/server.properties
```

#### 2. Buat Topic Kafka

Buka terminal ketiga untuk membuat topic bernama `uji-praktikum`.
```shell
./bin/kafka-topics.sh --create --topic uji-praktikum --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

#### 3. Kirim Pesan (Producer)

Di terminal yang sama, jalankan console producer dan ketik beberapa pesan.
```shell
./bin/kafka-console-producer.sh --topic uji-praktikum --bootstrap-server localhost:9092
> Halo Kafka!
> Ini pesan pertama saya.
```

#### 4. Baca Pesan (Consumer)

Buka terminal keempat untuk menjalankan console consumer.
```shell
./bin/kafka-console-consumer.sh --topic uji-praktikum --from-beginning --bootstrap-server localhost:9092
```
Anda akan melihat semua pesan yang dikirim oleh producer.

---
---

## Modul 4: Pra-Pemrosesan Data dengan PySpark

Modul ini dirancang untuk dijalankan di lingkungan seperti Google Colab. Semua kode, mulai dari persiapan hingga latihan, telah disediakan dalam satu file Python.

### Lokasi File

-   **`modul_4_preprocessing.py`**

### Cara Menggunakan

1.  Buka Google Colab (atau lingkungan notebook Jupyter lainnya dengan PySpark).
2.  Salin dan tempel isi dari file `modul_4_preprocessing.py` ke dalam sel-sel notebook.
3.  Jalankan sel instalasi terlebih dahulu: `!pip install pyspark findspark`.
4.  Jalankan sisa sel secara berurutan untuk mengikuti alur praktikum.

### Topik yang Dicakup

-   **Data Cleaning**: Menangani nilai yang hilang (missing values), data duplikat, dan data yang tidak konsisten/berisik.
-   **Data Transformasi**: Melakukan standarisasi (StandardScaler), normalisasi (MinMaxScaler), agregasi, dan diskretisasi (Bucketizer).
-   **Feature Engineering**: Mengekstraksi fitur dari tanggal/waktu, melakukan encoding pada variabel kategorikal (StringIndexer, OneHotEncoder), dan ekstraksi fitur teks (TF-IDF).
-   **Latihan & Tugas**: Menerapkan semua teknik yang telah dipelajari pada dataset baru.

## Modul 5: Analisis Statistik Deskriptif

Fokus pada eksplorasi data menggunakan dataset `diamonds`.

- **Load Data**: Konversi dari Pandas DataFrame (Seaborn load) ke Spark DataFrame.
- **Statistik Dasar**: Menggunakan `.describe()` untuk Mean, StdDev, Min, Max.
- **Statistik Lanjutan**: Menghitung Skewness, Variance, dan Modus menggunakan `pyspark.sql.functions`.
- **Median**: Membandingkan metode aproksimasi (`.summary`, `approxQuantile` error 0.01) dengan metode eksak (error 0.0) untuk memahami _trade-off_ performa pada Big Data.
- **Visualisasi**: Teknik sampling 10% data untuk visualisasi Histogram dan Box Plot menggunakan Matplotlib/Seaborn.

## Modul 6: Spark MLlib (Machine Learning)

Implementasi algoritma ML sederhana menggunakan Spark.

- **Feature Engineering**: Penggunaan `VectorAssembler` untuk menggabungkan fitur menjadi vektor tunggal.
- **Regression**: Prediksi Gaji berdasarkan Pengalaman dan Umur menggunakan `LinearRegression`.
- **Classification**: Deteksi Churn pelanggan menggunakan `LogisticRegression`.
- **Clustering**: Segmentasi pelanggan mall (Unsupervised) menggunakan `KMeans`.

## Modul 7: Kafka & Spark Structured Streaming

Simulasi pengolahan data transaksi toko secara _real-time_.

- **Setup Infrastruktur**: Instalasi Java 8, Kafka 3.6.0, dan Spark 3.5.0 di lingkungan Colab.
- **Kafka Producer**: Skrip Python untuk mengirim data dummy (JSON) ke topik `transaksi-toko`.
- **Spark Consumer**: Menggunakan Spark Structured Streaming dengan paket `spark-sql-kafka` untuk membaca stream.
- **Analisis Stream**: Agregasi total penjualan (Revenue) per produk secara _real-time_ dan menyimpannya ke _Memory Sink_ untuk di-query.

## 🚀 Cara Menjalankan (Google Colab)

1. **Instalasi Awal**:
   Pastikan menjalankan perintah instalasi `pyspark`, `findspark`, dan `kafka-python` di sel pertama.

   ```python
   !pip install pyspark findspark kafka-python
   ```

2. **Menjalankan Kafka (Khusus Modul 7)**:
   Kafka memerlukan Zookeeper dan Broker berjalan di background. Gunakan nohup agar servis tidak mati.

   ```bash
   !nohup $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > /dev/null 2>&1 &
   !nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > /dev/null 2>&1 &
   ```

3. **Dependency Spark-Kafka**:
   Saat menginisialisasi SparkSession untuk streaming, pastikan menyertakan config jar:
   ```python
   .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
   ```

## 📝 Tugas Latihan

- **Statistik**: Analisis perbandingan harga berlian berdasarkan warna dan histogram kedalaman (depth).
- **ML**: Prediksi gaji untuk data baru dan eksperimen jumlah cluster (K) pada K-Means.
- **Streaming**: Menambahkan metrik "Total Quantity" dan memfilter transaksi bernilai tinggi (> 1 Juta).
