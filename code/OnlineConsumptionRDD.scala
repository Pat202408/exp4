import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OnlineConsumptionRDD {
  def main(args: Array[String]): Unit = {

    // 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("OnlineConsumptionRDD")
      .master("local[*]")
      .getOrCreate()

    // 拿到 SparkContext
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取 CSV 文件
    val data = sc.textFile("input/ccf_online_stage1_train.csv")

    // 假设 CSV 有表头
    val header = data.first()
    val records = data.filter(_ != header)

    // 解析每行
    val merchantCounts = records.map { line =>
      val splits = line.split(",")
      val merchantId = splits(1)
      val couponId = splits(3)
      val date = splits(6)

      val neg = if ((date == "null" || date.isEmpty) && couponId != "null" && couponId.nonEmpty) 1 else 0
      val normal = if (date != "null" && date.nonEmpty && (couponId == "null" || couponId.isEmpty)) 1 else 0
      val pos = if (date != "null" && date.nonEmpty && couponId != "null" && couponId.nonEmpty) 1 else 0

      (merchantId, (neg, normal, pos))
    }

    // 聚合
    val result = merchantCounts
      .reduceByKey { case ((neg1, norm1, pos1), (neg2, norm2, pos2)) =>
        (neg1 + neg2, norm1 + norm2, pos1 + pos2)
      }

    // 转 DataFrame 并排序
    val df = result.map { case (mid, (neg, norm, pos)) =>
      (mid, neg, norm, pos)
    }.toDF("Merchant_id", "Negative", "Normal", "Positive")
      .orderBy("Merchant_id")

    // 保存为 CSV
    df.coalesce(1)
      .write
      .option("header", "true")
      .csv("output/online_consumption_table")

    // 打印前 10 行
    df.show(10)

    // 关闭
    spark.stop()
  }
}


