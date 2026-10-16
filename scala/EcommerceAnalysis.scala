import org.apache.spark.sql.SparkSession

object EcommerceAnalysis {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RetailIQ")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sparkContext

    val path =
      "file:///home/satwiqreddy/SparkProject/data/events.csv"

    // Task 1: Create an RDD from the CSV file
    val lines = sc.textFile(path)

    println("RDD created successfully")
    println("Total lines including header: " + lines.count())
    println("First line: " + lines.first())
    println("Number of input partitions: " + lines.getNumPartitions)

    spark.stop()
  }
}
