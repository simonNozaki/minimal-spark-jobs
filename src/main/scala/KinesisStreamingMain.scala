import com.amazonaws.auth.{AWSStaticCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.Record
import org.apache.hadoop.shaded.com.nimbusds.jose.util.StandardCharset
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.LocalDateTime
import org.apache.spark.streaming.kinesis.KinesisInitialPositions

/**
 * Kinesis Data Stream 監視ジョブ
 */
object KinesisStreamingMain {
  private val streamName = "dev-todo-items-data-stream"
  private val checkpointAppName = "dev-todo-items-stream-checkpoint"

  def main(sysArgs: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("kinesis-streaming-job")
    sparkConf.setIfMissing("spark.master", "local[*]")
    // 1sec置きにストリームを監視
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials
    val kinesisClient = AmazonKinesisClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))
      .build()
    val shardCounts = kinesisClient
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

        val reductions = rdd.map { strings =>
          strings.map { string => s"$string," }
        }.map { jsons => {
          // JSONが改行されて一つずつやってくるのでつなげて1行に縮約
          jsons.reduce { (jsonL: String, jsonR: String) => jsonL + jsonR}
        }}

        reductions.foreach { json => println(json)}
      }}

    ssc.start
    ssc.awaitTermination
  }
}
