# minimal-spark-jobs
Apache SparkのHello World。

- `JsonEtlMain`: spark sql、ジョインしてcsvにする
- `KinesisStreamingMain`: AWS Kinesis のストリームを受信して加工・json書き出し

## DDB -> Kinesis
流れてくるイベントをreduceした結果

```json
{"awsRegion":"ap-northeast-1","eventID":"6df61d08-6f37-4b70-a21a-ab276e3a8430","eventName":"INSERT","userIdentity":null,"recordFormat":"application/json","tableName":"dev-todo-items","dynamodb":{"ApproximateCreationDateTime":1660708268605,"Keys":{"id":{"S":"000001111122222"}},"NewImage":{"updatedAt":{"S":"2022-06-30T16:28:30.911+09:00"},"timestamp":{"N":"1656574110"},"createdAt":{"S":"2022-06-30T16:28:30.911+09:00"},"updatedBy":{"S":"script"},"id":{"S":"000001111122222"},"createdBy":{"S":"script"},"state":{"S":"DONE"},"title":{"S":"資料作成"}},"SizeBytes":181},"eventSource":"aws:dynamodb"},
{"awsRegion":"ap-northeast-1","eventID":"df4f4f82-1220-4208-9e70-8dca5bdede79","eventName":"MODIFY","userIdentity":null,"recordFormat":"application/json","tableName":"dev-todo-items","dynamodb":{"ApproximateCreationDateTime":1660708496994,"Keys":{"id":{"S":"000001111122222"}},"NewImage":{"updatedAt":{"S":"2022-06-30T16:28:30.911+09:00"},"timestamp":{"N":"1656574110"},"createdAt":{"S":"2022-06-30T16:28:30.911+09:00"},"updatedBy":{"S":"console"},"id":{"S":"000001111122222"},"createdBy":{"S":"consol"},"state":{"S":"DONE"},"title":{"S":"資料作成"}},"OldImage":{"updatedAt":{"S":"2022-06-30T16:28:30.911+09:00"},"timestamp":{"N":"1656574110"},"createdAt":{"S":"2022-06-30T16:28:30.911+09:00"},"updatedBy":{"S":"script"},"id":{"S":"000001111122222"},"createdBy":{"S":"script"},"state":{"S":"DONE"},"title":{"S":"資料作成"}},"SizeBytes":346},"eventSource":"aws:dynamodb"},
```

## お役立ち
- https://supergloo.com/spark-streaming/spark-kinesis-example/
- https://www.programcreek.com/scala/com.amazonaws.services.kinesis.model.Record
