import com.amazonaws.auth.{AWSStaticCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.LocalDateTime

/**
 * Kinesis Data Stream 監視ジョブ
 */
object KinesisStreamingMain {
  /**
   * セッションシングルトンオブジェクト
   */
  private object SparkSessionSingleton {
    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

  private val streamName = "dev-todo-items-data-stream"

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
        .streamName("dev-todo-items-data-stream")
        .checkpointAppName("dev-todo-items-data-stream-app")
        .build()
    }

    ssc
      .union(kinesisStreams)
      .flatMap(byteArray => new String(byteArray).split(","))
      .foreachRDD{rdd: RDD[String] => {
        println(s"${LocalDateTime.now.toString} : ストリーミング中...")
        val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import sparkSession.implicits._

        val df = rdd.toDF()
        df.write.format("console")
      }}

    ssc.start
    ssc.awaitTermination
  }
}
