# intel
cd /usr/local/Cellar/apache-spark/3.3.1/
# apple silicon
cd /opt/homebrew/Cellar/apache-spark/3.3.1/

SPARK_MASTER_HOST=192.168.0.6 ./libexec/sbin/start-master.sh

# twice
./libexec/bin/spark-class org.apache.spark.deploy.worker.Worker -c 2 -m 4g spark://192.168.0.6:7077

# panalgo
./libexec/bin/spark-shell --master spark://192.168.0.6:7077
