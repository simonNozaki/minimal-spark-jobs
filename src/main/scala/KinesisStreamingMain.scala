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

  def main(sysArgs: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("kinesis-streaming-job")
    // 1sec置きにストリームを監視
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // エンドポイントがデフォルトで "https://kinesis.us-east-1.amazonaws.com" になっている...
    // ルールに沿って定義必要: https://docs.aws.amazon.com/general/latest/gr/rande.html
    val stream = KinesisInputDStream.builder
      .streamingContext(ssc)
      .endpointUrl("https://kinesis.ap-northeast-1.amazonaws.com")
      .regionName("ap-northeast-1")
      .streamName("dev-todo-items-data-stream")
      .checkpointAppName("dev-todo-items-data-stream-app")
      .build()

    stream.flatMap(b => b.mkString("Array(", ", ", ")")).foreachRDD{rdd: RDD[Char] => {
      println(s"${LocalDateTime.now.toString} : ストリーミング中...")
      val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import sparkSession.implicits._

      val data = new String(rdd.collect())
      println(data)
    }}

    ssc.start
    ssc.awaitTermination
  }
}
