## How to setup env
``docker compose up -d``

``cd crypto-kafka/``

``python crypto_producer.py``
## start park service manually
``docker-compose exec spark-consumer spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /opt/bitnami/python/src/spark/spark_consumer.py``

## ui SERVICES 
http://localhost:8082 -> Spark 
http://localhost:8081 -> Kafka
## Stacks uses
![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)
![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Spark](https://img.shields.io/badge/Apache_Spark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16)

## Docs
[![Notion](https://img.shields.io/badge/Notion-000000?style=for-the-badge&logo=notion&logoColor=white)
](https://www.notion.so/CRYPTO-VIZ-DATA-ffabd7eb239d4f50b5268836eeabd29d?pvs=4)
