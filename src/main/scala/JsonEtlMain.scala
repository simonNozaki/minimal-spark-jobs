import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession

import java.time.{LocalDateTime, ZoneId}

object JsonEtlMain {
  private val today = LocalDateTime.now(ZoneId.of("Asia/Tokyo")).toString
  /**
   * ユーザデータ
   */
  private val users = Seq(
    ("1", "Peter Tiel", today),
    ("2", "Rich Hickey", today),
    ("3", "Anders Hejlsberg", today),
  )
  /**
   * ユーザテーブル定義
   */
  private val activeUserColumns = Seq("id", "name", "registered_at")

  /**
   * DataFrame 書き込みモード トレイト
   */
  private sealed trait WriteMode
  private case object WriteModeAppend extends WriteMode
  private case object WriteModeIgnore extends WriteMode
  private case object WriteModeOverwrite extends WriteMode

  /**
   * DataFrame 書き込みモードを返す
   * @param writeMode
   * @return
   */
  private def getWriteMode(writeMode: WriteMode): String = {
    writeMode match {
      case WriteModeIgnore => "ignore"
      case WriteModeAppend => "append"
      case WriteModeOverwrite => "overwrite"
    }
  }

  private def writeJoinedData(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    // toDF はimplicitな関数
    val activeUserDf = users.toDF(activeUserColumns:_*)
    activeUserDf.show

    // 2.3以降はdf#showのためにmultiline対応しないといけない
    // https://itecnote.com/tecnote/json-since-spark-2-3-the-queries-from-raw-json-csv-files-are-disallowed-when-the-referenced-columns-only-include-the-internal-corrupt-record-column/
    // JSONファイルのパスはクラスパスではない、ルートからの相対パス
    val todosDf = sparkSession
      .read
      .option("multiline","true")
      .json("src/main/resources/todo-items.json")
    todosDf.show
    todosDf.printSchema

    val todoWithUserDf = todosDf
      .join(activeUserDf, todosDf("userId") === activeUserDf("id"))
      .drop(col("userId"))
    todoWithUserDf.show

    todosDf.createTempView("todos")

    //    root
    //    |-- createdAt: string (nullable = true)
    //    |-- createdBy: string (nullable = true)
    //    |-- id: string (nullable = true)
    //    |-- state: string (nullable = true)
    //    |-- timestamp: string (nullable = true)
    //    |-- title: string (nullable = true)
    //    |-- updatedAt: string (nullable = true)
    //    |-- updatedBy: string (nullable = true)

    // mode: overwrite, ignore, append
    sparkSession.sql("select * from todos")
      .write
      .mode(getWriteMode(WriteModeOverwrite))
      .csv("src/main/resources/o")
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("todo-items-etl-jobs")
      .getOrCreate
    this.writeJoinedData(sparkSession)
  }
}
