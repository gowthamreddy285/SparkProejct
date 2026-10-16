import org.apache.spark.sql.SparkSession

object HelloSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EcommerceRDDTest")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val numbers = sc.parallelize(1 to 10, 4)
    val squares = numbers.map(n => n * n)

    println("Squares: " + squares.collect().mkString(", "))
    println("Partitions: " + numbers.getNumPartitions)

    spark.stop()
  }
}
