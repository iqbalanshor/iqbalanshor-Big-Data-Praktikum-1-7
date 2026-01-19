#!/usr/bin/env python
import sys
from itertools import groupby

# Membaca semua baris dari input standar (stdin)
for key, group in groupby(sys.stdin, key=lambda x: x.split('\t', 1)[0]):
 try:
 # Jumlahkan semua hitungan (angka 1) untuk setiap kata
 total_count = sum(int(line.split('\t', 1)[1].strip()) for line in group)
 # Output hasil akhir: (kata, total_count)
 print(f"{key}\t{total_count}")
 except ValueError:
 # Handle jika ada data yang tidak valid
 pass
