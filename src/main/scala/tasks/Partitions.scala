package tasks

import org.apache.spark.sql.SparkSession

// Show Partitions
object Partitions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Partitions")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val dataPath = "file:///Users/kireeti/Desktop/Projects/SparkProejct/data/events.csv"

    println("\nTASK 5: PARTITIONS")

    // Load data
    val rawRDD = sc.textFile(dataPath)
    println(s"\n1. Raw RDD partitions: ${rawRDD.getNumPartitions}")

    val header = rawRDD.first()
    val dataRDD = rawRDD.filter(line => line != header)
    println(s"2. After filter partitions: ${dataRDD.getNumPartitions}")

    // Create event counts RDD
    val eventsRDD = dataRDD.map { line =>
      val fields = line.split(",")
      fields(2)
    }

    val eventCounts = eventsRDD
      .map(event => (event, 1))
      .reduceByKey(_ + _)

    println(s"3. Event counts RDD partitions: ${eventCounts.getNumPartitions}")

    // Repartition example
    val repartitioned = dataRDD.repartition(5)
    println(s"\n4. After repartition(5): ${repartitioned.getNumPartitions}")

    println("\nWhat are partitions?")
    println("- Spark splits data into chunks called partitions")
    println("- Each partition is processed independently")
    println("- More partitions = more parallelism")
    println("- Default partitions usually = number of CPU cores")

    println("\nPARTITIONS DEMO COMPLETE\n")

    spark.stop()
  }
}
