aws dynamodb put-item \
  --table-name local-todo-items \
  --endpoint-url http://localhost:4566 \
  --item '
         {
           "id": {
             "S": "000001111122222"
           },
           "timestamp": {
             "N": "1656574110"
           },
           "title": {
             "S": "資料作成"
           },
           "state": {
             "S": "DONE"
           },
           "createdAt": {
             "S": "2022-06-30T16:28:30.911+09:00"
           },
           "createdBy": {
             "S": "script"
           },
           "updatedAt": {
             "S": "2022-06-30T16:28:30.911+09:00"
           },
           "updatedBy": {
             "S": "script"
           }
         }
         '

aws dynamodb scan --endpoint-url http://localhost:4566 --table-name local-todo-items
