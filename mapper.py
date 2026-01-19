#!/usr/bin/env python
import sys

# Membaca setiap baris dari input standar (stdin)
for line in sys.stdin:
 # Hapus spasi di awal/akhir dan pisahkan kata
 words = line.strip().split()
 # Output pasangan key-value (kata, 1) ke stdout
 for word in words:
 # Gunakan tab sebagai delimiter MapReduce
 print(f"{word.lower()}\t1")
