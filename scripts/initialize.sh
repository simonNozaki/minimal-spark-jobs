echo テーブルの作成を開始します
./create_tables.sh
echo テーブルの作成を終了します

echo ストリームの作成を開始します
./enable_kinesis_stream.sh
echo ストリームの作成を終了します

echo レコードの登録を開始します
./put_records.sh
echo レコードの登録を終了します

