kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic highest-traded-stock --config "cleanup.policy=compact"
:: --config "delete.retention.ms=100"  --config "segment.ms=100"