
import org.apache.spark.sql.SparkSession

object EcommerceAnalysis {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("EcommerceCustomerBehaviorAnalysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sparkContext

    val path = "file:///home/satwiqreddy/SparkProject/data/events.csv"

    // 1. Create an RDD from the CSV file
    val lines = sc.textFile(path)
    val header = lines.first()
    val data = lines.filter(line => line != header)

    // 2. Transformations: parse, filter and map
    val events = data.map(_.split(",", -1))
      .filter(row => row.length >= 5 && row(1).nonEmpty &&
        row(2).nonEmpty && row(3).nonEmpty)
      .map(row => (
        row(1).toInt,
        (row(2), row(3).toInt)
      ))

    // 3. Key-value RDD: count events per visitor
    val visitorEventCounts = events
      .mapValues(_ => 1)
      .reduceByKey(_ + _)
      .cache()

    // 4. Actions
    println("Total event records: " + events.count())
    println("Unique visitors: " + visitorEventCounts.count())

    // 5. Event type counts
    val eventCounts = data
      .map(_.split(",", -1))
      .filter(row => row.length >= 3)
      .map(row => (row(2), 1))
      .reduceByKey(_ + _)

    println("Event type counts:")
    eventCounts.collect().foreach(println)

    // 6. Partition information
    println("Input partitions: " + data.getNumPartitions)
    println("Visitor-count partitions: " +
      visitorEventCounts.getNumPartitions)

    // 7. Lineage / DAG information
    println("RDD lineage:")
    println(visitorEventCounts.toDebugString)

    // 8. Example visitor results
    println("Top 10 visitors by event count:")
    visitorEventCounts
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach(println)

    spark.stop()
  }
}
