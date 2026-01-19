# Modul Praktikum 4: Pra-Pemrosesan Data dengan PySpark
# File ini berisi semua kode yang dijelaskan dalam modul praktikum.
# Anda dapat menjalankan ini di lingkungan PySpark seperti Google Colab.

# Langkah 0: Persiapan Lingkungan
# Di Google Colab, jalankan baris berikut di sel terpisah:
# !pip install pyspark findspark

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, MinMaxScaler, Bucketizer, Tokenizer, HashingTF, IDF, StringIndexer, OneHotEncoder

# Membuat SparkSession
spark = SparkSession.builder \
 .master("local[*]") \
 .appName("PraktikumPreprocessing") \
 .getOrCreate()

print("SparkSession berhasil dibuat!")
print("-" * 50)


# Langkah 1: Membuat Dataset Sampel
# Data sampel yang "kotor"
data_kotor = [
 (1, 'Budi Susanto', 25, 5500000, 'L', '2022-01-15', 'Jakarta', 'Transaksi berhasil, barang bagus'),
 (2, 'Ani Lestari', None, 8000000, 'P', '2022-02-20', 'Bandung', 'Pengiriman cepat dan barang sesuai'),
 (3, 'Candra Wijaya', 35, 12000000, 'L', '2022-01-18', 'Surabaya', 'Sangat puas dengan pelayanannya'),
 (4, 'Dewi Anggraini', 22, 4800000, 'P', '2022-03-10', 'JKT', 'Barang diterima dalam kondisi baik'),
 (5, 'Eka Prasetyo', 45, 15000000, 'L', '2022-04-01', 'Jakarta', 'Transaksi gagal, mohon diperiksa'),
 (6, 'Budi Susanto', 25, 5500000, 'L', '2022-01-15', 'Jakarta', 'Transaksi berhasil, barang bagus'), # Data duplikat
 (7, 'Fina Rahmawati', 29, 9500000, 'P', '2022-05-12', 'Bandung', None), # Missing value di ulasan
 (8, 'Galih Nugroho', 31, -7500000, 'L', '2022-06-25', 'Surabaya', 'Barang oke'), # Gaji tidak valid (noisy)
 (9, 'Hesti Wulandari', 55, 25000000, 'P', '2022-07-30', 'Jakarta', 'Pelayanan ramah dan cepat'),
 (10, 'Indra Maulana', 150, 6200000, 'L', '2022-08-05', 'Medan', 'Produk original') # Usia tidak valid (outlier)
]

# Mendefinisikan skema DataFrame
skema = StructType([
 StructField("id_pelanggan", IntegerType(), True),
 StructField("nama", StringType(), True),
 StructField("usia", IntegerType(), True),
 StructField("gaji", IntegerType(), True),
 StructField("jenis_kelamin", StringType(), True),
 StructField("tgl_registrasi", StringType(), True),
 StructField("kota", StringType(), True),
 StructField("ulasan", StringType(), True)
])

# Membuat DataFrame
df = spark.createDataFrame(data=data_kotor, schema=skema)

print("Dataset Awal:")
df.show(truncate=False)
df.printSchema()
print("-" * 50)


# BAGIAN 1: DATA CLEANING
print("BAGIAN 1: DATA CLEANING")
print("-" * 50)

# 1.1 Menangani Missing Values
print("1.1 Menangani Missing Values")
print("Jumlah missing values di setiap kolom:")
df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()

# Strategi: Mengisi nilai null (Imputasi)
mean_usia = df.select(mean(df['usia'])).collect()[0][0]
df_filled = df.na.fill({'usia': int(mean_usia)})
df_filled = df_filled.na.fill({'ulasan': 'Tidak ada ulasan'})
print("Data setelah imputasi:")
df_filled.show(truncate=False)
df_bersih = df_filled

# 1.2 Menangani Data Duplikat
print("\n1.2 Menangani Data Duplikat")
print(f"Jumlah baris sebelum hapus duplikat: {df_bersih.count()}")
df_bersih = df_bersih.dropDuplicates()
print(f"Jumlah baris setelah hapus duplikat: {df_bersih.count()}")

# 1.3 Memperbaiki Data Tidak Konsisten & Berisik (Noisy)
print("\n1.3 Memperbaiki Data Tidak Konsisten & Berisik")
# Standarisasi kolom 'kota'
df_bersih = df_bersih.withColumn("kota", when(df_bersih["kota"] == "JKT", "Jakarta").otherwise(df_bersih["kota"]))
# Memperbaiki data berisik
df_bersih = df_bersih.withColumn("gaji", abs(df_bersih["gaji"]))
df_bersih = df_bersih.filter(df_bersih["usia"] <= 100)
print("Data setelah perbaikan inkonsistensi dan noise (ini adalah df_bersih final):")
df_bersih.show(truncate=False)
print("-" * 50)


# BAGIAN 2: DATA TRANSFORMASI
print("BAGIAN 2: DATA TRANSFORMASI")
print("-" * 50)

# 2.1 Standarisasi & Normalisasi
print("2.1 Standarisasi & Normalisasi")
assembler = VectorAssembler(inputCols=["usia", "gaji"], outputCol="fitur_numerik")
df_vector = assembler.transform(df_bersih)

# Standarisasi
scaler_std = StandardScaler(inputCol="fitur_numerik", outputCol="fitur_standar", withStd=True, withMean=True)
df_standar = scaler_std.fit(df_vector).transform(df_vector)
print("Data setelah Standarisasi:")
df_standar.select("usia", "gaji", "fitur_standar").show(truncate=False)

# Normalisasi
scaler_minmax = MinMaxScaler(inputCol="fitur_numerik", outputCol="fitur_normal")
df_normal = scaler_minmax.fit(df_vector).transform(df_vector)
print("Data setelah Normalisasi:")
df_normal.select("usia", "gaji", "fitur_normal").show(truncate=False)

# 2.2 Agregasi Data
print("\n2.2 Agregasi Data")
df_agregat = df_bersih.groupBy("kota").agg(
 count("id_pelanggan").alias("jumlah_pelanggan"),
 avg("gaji").alias("rata_rata_gaji")
)
print("Hasil Agregasi per Kota:")
df_agregat.show()

# 2.3 Diskretisasi (Binning)
print("\n2.3 Diskretisasi (Binning)")
splits = [0, 20, 40, float('Inf')]
bucketizer = Bucketizer(splits=splits, inputCol="usia", outputCol="kelompok_usia")
df_binned = bucketizer.transform(df_bersih)
print("Data setelah Diskretisasi Usia:")
df_binned.select("usia", "kelompok_usia").show()
print("-" * 50)


# BAGIAN 3: FEATURE ENGINEERING
print("BAGIAN 3: FEATURE ENGINEERING")
print("-" * 50)

# 3.1 Ekstraksi Fitur dari Tanggal & Waktu
print("3.1 Ekstraksi Fitur dari Tanggal & Waktu")
df_engineered = df_bersih.withColumn("timestamp_reg", to_timestamp("tgl_registrasi", "yyyy-MM-dd"))
df_engineered = df_engineered.withColumn("tahun_reg", year("timestamp_reg"))
df_engineered = df_engineered.withColumn("bulan_reg", month("timestamp_reg"))
df_engineered = df_engineered.withColumn("hari_dalam_minggu", dayofweek("timestamp_reg"))
print("Data setelah ekstraksi fitur tanggal:")
df_engineered.select("tgl_registrasi", "tahun_reg", "bulan_reg", "hari_dalam_minggu").show()

# 3.2 Encoding Variabel Kategorikal
print("\n3.2 Encoding Variabel Kategorikal")
indexer_jk = StringIndexer(inputCol="jenis_kelamin", outputCol="jk_index")
indexer_kota = StringIndexer(inputCol="kota", outputCol="kota_index")
df_indexed = indexer_jk.fit(df_engineered).transform(df_engineered)
df_indexed = indexer_kota.fit(df_indexed).transform(df_indexed)
ohe = OneHotEncoder(inputCols=["jk_index", "kota_index"], outputCols=["jk_ohe", "kota_ohe"])
df_encoded = ohe.fit(df_indexed).transform(df_indexed)
print("Data setelah One-Hot Encoding:")
df_encoded.select("jenis_kelamin", "jk_index", "jk_ohe", "kota", "kota_index", "kota_ohe").show(truncate=False)

# 3.3 Ekstraksi Fitur dari Teks (TF-IDF)
print("\n3.3 Ekstraksi Fitur dari Teks (TF-IDF)")
tokenizer = Tokenizer(inputCol="ulasan", outputCol="kata")
df_tokenized = tokenizer.transform(df_encoded)
hashingTF = HashingTF(inputCol="kata", outputCol="tf_raw", numFeatures=20)
df_tf = hashingTF.transform(df_tokenized)
idf = IDF(inputCol="tf_raw", outputCol="fitur_tfidf")
df_tfidf = idf.fit(df_tf).transform(df_tf)
print("Data setelah Ekstraksi Fitur TF-IDF dari Ulasan:")
df_tfidf.select("ulasan", "fitur_tfidf").show(truncate=False)
print("-" * 50)


# BAGIAN 4: LATIHAN DAN TUGAS
print("BAGIAN 4: LATIHAN DAN TUGAS")
print("-" * 50)

# 4.1 Latihan
print("4.1 Latihan")

# 1. Agregasi Lanjutan
print("\nLatihan 1: Agregasi Lanjutan")
df_latihan_1 = df_bersih.groupBy("jenis_kelamin", "kota").agg(
 max("gaji").alias("gaji_maksimum"),
 min("usia").alias("usia_minimum")
)
df_latihan_1.show()

# 2. Diskretisasi Gaji
print("\nLatihan 2: Diskretisasi Gaji")
splits_gaji = [-float('Inf'), 7000000, 15000000, float('Inf')]
bucketizer_gaji = Bucketizer(splits=splits_gaji, inputCol="gaji", outputCol="level_gaji_index")
df_latihan_2 = bucketizer_gaji.transform(df_bersih)
# Memberi label yang lebih deskriptif
df_latihan_2 = df_latihan_2.withColumn("level_gaji",
 when(col("level_gaji_index") == 0.0, "Rendah")
 .when(col("level_gaji_index") == 1.0, "Menengah")
 .otherwise("Tinggi")
)
df_latihan_2.select("gaji", "level_gaji").show()

# 3. Feature Engineering Sederhana
print("\nLatihan 3: Feature Engineering Sederhana")
df_latihan_3 = df_bersih.withColumn("usia_x_gaji", col("usia") * col("gaji"))
df_latihan_3.select("id_pelanggan", "usia", "gaji", "usia_x_gaji").show(5)
print("-" * 50)


# 4.2 Tugas
print("4.2 Tugas: Alur Pra-pemrosesan Data Produk Elektronik")
data_produk = [
 (101, 'Laptop A', 'Elektronik', 15000000, 4.5, 120, '2023-01-20', 'stok_tersedia'),
 (102, 'Smartphone B', 'Elektronik', 8000000, 4.7, 250, '2023-02-10', 'stok_tersedia'),
 (103, 'Headphone C', 'Aksesoris', 1200000, 4.2, None, '2023-02-15', 'stok_habis'),
 (104, 'Laptop A', 'Elektronik', 15000000, 4.5, 120, '2023-01-20', 'stok_tersedia'), # Duplikat
 (105, 'Tablet D', 'Elektronik', 6500000, None, 80, '2023-03-01', 'stok_tersedia'),
 (106, 'Charger E', 'Aksesoris', 250000, -4.0, 500, '2023-03-05', 'Stok_Tersedia'), # Rating tidak valid & Status inkonsisten
 (107, 'Smartwatch F', 'Elektronik', 3100000, 4.8, 150, '2023-04-12', 'stok_habis')
]
skema_produk = StructType([
 StructField("id_produk", IntegerType()), StructField("nama_produk", StringType()),
 StructField("kategori", StringType()), StructField("harga", IntegerType()),
 StructField("rating", FloatType()), StructField("terjual", IntegerType()),
 StructField("tgl_rilis", StringType()), StructField("status_stok", StringType())
])
df_tugas = spark.createDataFrame(data=data_produk, schema=skema_produk)
print("Dataset Tugas Awal:")
df_tugas.show()

# 1. Data Cleaning
# Imputasi
mean_terjual = df_tugas.select(mean("terjual")).collect()[0][0]
mean_rating = df_tugas.select(mean("rating")).collect()[0][0]
df_tugas_bersih = df_tugas.na.fill({'terjual': int(mean_terjual), 'rating': float(mean_rating)})
# Hapus duplikat
df_tugas_bersih = df_tugas_bersih.dropDuplicates()
# Perbaiki nilai tidak valid
df_tugas_bersih = df_tugas_bersih.withColumn("rating", abs(col("rating")))
# Standarisasi status_stok
df_tugas_bersih = df_tugas_bersih.withColumn("status_stok", lower(col("status_stok")))

print("\nDataFrame Tugas Setelah Cleaning:")
df_tugas_bersih.show()

# 2. Data Transformasi
# Standarisasi
assembler_tugas = VectorAssembler(inputCols=["harga", "rating", "terjual"], outputCol="fitur")
df_tugas_vector = assembler_tugas.transform(df_tugas_bersih)
scaler_tugas = StandardScaler(inputCol="fitur", outputCol="fitur_standar")
df_tugas_standar = scaler_tugas.fit(df_tugas_vector).transform(df_tugas_vector)

print("\nDataFrame Tugas Setelah Standarisasi:")
df_tugas_standar.select("harga", "rating", "terjual", "fitur_standar").show()

# 3. Feature Engineering
# Ekstrak bulan
df_tugas_engineered = df_tugas_standar.withColumn("bulan_rilis", month(to_timestamp("tgl_rilis", "yyyy-MM-dd")))
# Encoding
indexer_kategori = StringIndexer(inputCol="kategori", outputCol="kategori_index")
indexer_status = StringIndexer(inputCol="status_stok", outputCol="status_index")
df_tugas_indexed = indexer_kategori.fit(df_tugas_engineered).transform(df_tugas_engineered)
df_tugas_indexed = indexer_status.fit(df_tugas_indexed).transform(df_tugas_indexed)
ohe_tugas = OneHotEncoder(inputCols=["kategori_index", "status_index"], outputCols=["kategori_ohe", "status_ohe"])
df_tugas_final = ohe_tugas.fit(df_tugas_indexed).transform(df_tugas_indexed)

# 4. Tampilkan Hasil Akhir
print("\nHasil Akhir DataFrame Tugas:")
df_tugas_final.select(
 "id_produk", "nama_produk", "kategori", "harga", "rating", "terjual",
 "bulan_rilis", "kategori_ohe", "status_ohe", "fitur_standar"
).show(truncate=False)
print("-" * 50)


# Langkah Terakhir: Menghentikan SparkSession
spark.stop()
print("SparkSession telah dihentikan.")
