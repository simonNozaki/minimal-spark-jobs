import com.amazonaws.auth.{AWSStaticCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord
import com.amazonaws.services.s3.AmazonS3Client
import com.google.gson.Gson
import org.apache.hadoop.shaded.com.nimbusds.jose.util.StandardCharset
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.LocalDateTime
import org.apache.spark.streaming.kinesis.KinesisInitialPositions

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.util.{Failure, Success, Try}

/**
 * Kinesis Data Stream 監視ジョブ
 */
object KinesisStreamingMain {
  private val streamName = "dev-todo-items-data-stream"
  private val checkpointAppName = "dev-todo-items-stream-checkpoint"

  /**
   * DynamoDBの変更キャプチャイベントを、Javaクラスにはまるようキャメルケースにする
   * @param str
   * @return
   */
  private def fromUpperCamelCaseToCamelCase(str: String) = str
    .replace("NewImage", "newImage")
    .replace("OldImage", "oldImage")
    .replace("\"S\"", "\"s\"")
    .replace("\"N\"", "\"n\"")
    .replace("\"Keys\"", "\"keys\"")

  /**
   * 最後の一字を削除
   * @param str
   * @return
   */
  private def removeLast(str: String): String = str.substring(0, str.length - 1)

  private val gson = new Gson

  // ------------------
  // AWS
  // ------------------
  private val defaultCredentialProvider = new AWSStaticCredentialsProvider(new DefaultAWSCredentialsProviderChain().getCredentials)

  private val s3 = AmazonS3Client
    .builder()
    .withCredentials(defaultCredentialProvider)
    .build()
  private val kinesis = AmazonKinesisClientBuilder
    .standard()
    .withCredentials(defaultCredentialProvider)
    .build()

  private val bucket = "dev-todo-items"

  /**
   * Todo要素
   * @param id
   * @param state
   * @param title
   * @param createdAt
   * @param createdBy
   * @param updatedAt
   * @param updatedBy
   */
  private case class TodoItem(id: String,
                              timestamp: Int,
                              state: String,
                              title: String,
                              createdAt: String,
                              createdBy: String,
                              updatedAt: String,
                              updatedBy: String)

  /**
   * DynamoDBストリームイベントからクラスの生成
   * @param streamRecord
   * @return
   */
  private def createTodoItem(streamRecord: DynamodbStreamRecord): TodoItem = {
    val newImage = streamRecord.getDynamodb.getNewImage
    TodoItem(
      id = newImage.get("id").getS,
      timestamp = newImage.get("timestamp").getN.toInt,
      state = newImage.get("state").getS,
      title = newImage.get("title").getS,
      createdAt = newImage.get("createdAt").getS,
      createdBy = newImage.get("createdBy").getS,
      updatedAt = newImage.get("updatedAt").getS,
      updatedBy = newImage.get("updatedBy").getS
    )
  }

  def main(sysArgs: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("kinesis-streaming-job")
    sparkConf.setIfMissing("spark.master", "local[*]")
    // 1sec置きにストリームを監視
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val shardCounts = this.kinesis
      .describeStream(streamName)
      .getStreamDescription
      .getShards
      .size

    // エンドポイントがデフォルトで "https://kinesis.us-east-1.amazonaws.com" になっている...
    // ルールに沿って定義必要: https://docs.aws.amazon.com/general/latest/gr/rande.html
    val kinesisStreams = (0 until shardCounts).map { i =>
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .endpointUrl("https://kinesis.ap-northeast-1.amazonaws.com")
        .regionName("ap-northeast-1")
        .streamName(streamName)
        .checkpointAppName(checkpointAppName)
        .initialPosition(new KinesisInitialPositions.TrimHorizon)
        .buildWithMessageHandler((record: Record) => {
          StandardCharset.UTF_8.decode(record.getData).toString.split(",")
        })
    }

    ssc
      .union(kinesisStreams)
      .foreachRDD{rdd: RDD[Array[String]] => {
        println(s"${LocalDateTime.now.toString} : ストリーミング中...")

        // ストリームデータを文字列に直す
        val reductions = rdd.map { strings =>
          strings.map { string => s"$string," }
        }.map { jsons => {
          // JSONが改行されて一つずつやってくるのでつなげて1行に縮約
          val reduced = jsons.reduce { (jsonL: String, jsonR: String) => jsonL + jsonR}
          // 最後のカンマいらない
          this.removeLast(reduced)
        }}

        reductions.foreach { r => println(r)}

        // StreamRecordにする
        val streamRecords = reductions
          .map { reduction => fromUpperCamelCaseToCamelCase(reduction) }
          .map { reduction => {
            val record = gson.fromJson(reduction, classOf[DynamodbStreamRecord])
            println(record)
            record
          }}

        // JSONファイルにする
        val files = streamRecords
          .map { r => createTodoItem(r)}
          .map { todoItem => {
            val newFile = Paths.get(s"${LocalDateTime.now()}_${UUID.randomUUID().toString}.json")
            Files.writeString(newFile, gson.toJson(todoItem)).toFile
          }}

        // s3に置く
        files
          .map { file => Try(s3.putObject(bucket, file.getName, file))}
          .foreach {
            case Success(value) => println(s"バケットへのpushに成功しました: ${value.toString}")
            case Failure(exception) => println(s"バケットへのpushに失敗しました: ${exception.printStackTrace()}")
          }

      }}

    ssc.start
    ssc.awaitTermination
  }
}
