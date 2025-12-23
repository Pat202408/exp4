import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CouponUsageTimeDistribution {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CouponUsageTimeDistribution")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("input/ccf_offline_stage1_train.csv")

    val usedDF = df.filter($"Date" =!= "null" && $"Coupon_id" =!= "null")
    val dayDF = usedDF.withColumn("Day", substring($"Date", 7, 2).cast("int"))

    val categorizedDF = dayDF.withColumn("Period",
      when($"Day" <= 10, "early")
        .when($"Day" <= 20, "mid")
        .otherwise("late")
    )

    val countDF = categorizedDF.groupBy("Coupon_id", "Period").count()
    val totalDF = usedDF.groupBy("Coupon_id").count().withColumnRenamed("count", "total_count")
    val joinedDF = countDF.join(totalDF, "Coupon_id")
      .withColumn("prob", $"count" / $"total_count")

    val resultDF = joinedDF.groupBy("Coupon_id")
      .pivot("Period", Seq("early", "mid", "late"))
      .agg(first("prob"))
      .na.fill(0.0) 

    resultDF.coalesce(1)
      .write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("output/coupon_usage_time_distribution_tmp")
  }
}

