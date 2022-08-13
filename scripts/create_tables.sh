# テーブル作成: dev-auth-company-valuation-info
aws dynamodb create-table \
  --endpoint-url http://localhost:4566 \
  --table-name local-todo-items \
  --attribute-definitions \
      AttributeName=id,AttributeType=S \
      AttributeName=timestamp,AttributeType=N \
  --key-schema AttributeName=id,KeyType=HASH AttributeName=timestamp,KeyType=RANGE \
  --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

echo テーブルを作成しました...

aws dynamodb list-tables \
  --endpoint-url http://localhost:4566
