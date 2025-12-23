
import org.apache.spark.sql.SparkSession

object TopMerchantsSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TopMerchantsSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("sep", ",")
      .option("header", "true")
      .csv("input/online_consumption_table.csv")
      .toDF("Merchant_id", "Negative", "Normal", "Positive")

    val dfInt = df.withColumn("Negative", $"Negative".cast("int"))
      .withColumn("Normal", $"Normal".cast("int"))
      .withColumn("Positive", $"Positive".cast("int"))

    dfInt.createOrReplaceTempView("online_consumption")

    val top10 = spark.sql(
      """
        |SELECT
        |  Merchant_id,
        |  CAST(Positive AS DOUBLE)/(Negative + Normal + Positive) AS PosRatio,
        |  Positive,
        |  (Negative + Normal + Positive) AS Total
        |FROM online_consumption
        |ORDER BY PosRatio DESC
        |LIMIT 10
        |""".stripMargin)

    top10.show(false)

    spark.stop()
  }
}

