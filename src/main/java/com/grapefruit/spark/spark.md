# spark 相关命令
## 启动sprrk
```shell
    bin/saprk-shell
```

## 提交spark任务
```shell
    bin/spark-submit \
    --class com.grapefruit.spark.wordcount.WordCount \
    --master 'local[2]' \
    ./examples/jars/spark-0.0.1-SNAPSHOT.jar  \
    10
```

