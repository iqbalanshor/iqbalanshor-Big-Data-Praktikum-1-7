# module_7_colab_setup.md

# Setup Instructions for Google Colab (Module 7)

Copy and run the following commands in a Google Colab cell to set up the environment.

```python
# 1. Install Java 8
!apt-get install openjdk-8-jdk-headless -qq > /dev/null

# 2. Download & Extract Kafka 3.6.0
!wget -q https://archive.apache.org/dist/kafka/3.6.0/kafka_2.12-3.6.0.tgz
!tar -xzf kafka_2.12-3.6.0.tgz

# 3. Download & Extract Spark 3.5.0
!wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
!tar xf spark-3.5.0-bin-hadoop3.tgz

# 4. Install Python Dependencies
!pip install -q findspark
!pip install kafka-python

# 5. Set Environment Variables
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.5.0-bin-hadoop3"
os.environ["KAFKA_HOME"] = "/content/kafka_2.12-3.6.0"

# 6. Initialize findspark
import findspark
findspark.init()

# 7. Start Zookeeper & Kafka Server (Run as background daemons)
print("Starting Zookeeper and Kafka...")
!$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
!$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
print("Kafka Environment Ready!")

# 8. Create Topic 'transaksi-toko' (Optional, auto-create usually works)
!$KAFKA_HOME/bin/kafka-topics.sh --create --topic transaksi-toko --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```
