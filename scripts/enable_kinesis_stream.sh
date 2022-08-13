# Create kinesis data stream for DDB
aws kinesis create-stream \
    --endpoint-url http://localhost:4566 \
    --stream-name local-todo-items-data-stream \
    --shard-count 2

echo ストリームを作成しました

aws kinesis list-streams \
    --endpoint-url http://localhost:4566 \

# Enable kinesis data stream
aws dynamodb enable-kinesis-streaming-destination \
  --endpoint-url http://localhost:4566 \
  --table-name local-todo-items \
  --stream-arn "arn:aws:kinesis:us-east-1:000000000000:stream/local-todo-items-data-stream"
