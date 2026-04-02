# BigDataSpark

Команды необходимо выполнять поочередно (нельзя сделать витрины без снежинок).
Первым делом необходимо запустить контейнеры: ```docker compose up```

## Снежинка postgresql
Запускает джобу для создания снежинки.  
Можно добавить вместо PGSql YDB.    
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /jars/postgresql-42.7.10.jar /scripts/snowflake_etl.py
```


## Отчеты clickhouse
Изначально через SQL создается схема. Она включает в себя:  
- БД с отчетами, в которой: 
- - 6 витрин-отчетов с данными  
- - 18 view, которые собирают и выводят выборку для решения [задания](./ZADANIE.md)
  
БД используют движок Atomic, таблицы - MergeTree.   
Для работы со Spark необходим clickhouse-connector и postgresql-jdbs  
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /jars/postgresql-42.7.10.jar,/jars/clickhouse-spark-runtime-4.0_2.13-0.10.0.jar --conf spark.sql.catalog.clickhouse=com.clickhouse.spark.ClickHouseCatalog --conf spark.sql.catalog.clickhouse.host=clickhouse --conf spark.sql.catalog.clickhouse.http_port=8123 --conf spark.sql.catalog.clickhouse.user=meow --conf spark.sql.catalog.clickhouse.password=UwU  /scripts/spark_mart_gen.py
```

![alt text](image.png)
![alt text](image-1.png)
