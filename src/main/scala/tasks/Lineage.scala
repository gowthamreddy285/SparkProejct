package tasks

import org.apache.spark.sql.SparkSession

// RDD Lineage
object Lineage {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Lineage")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val dataPath = "file:///Users/kireeti/Desktop/Projects/SparkProejct/data/events.csv"

    println("\nTASK 6: RDD LINEAGE")

    // Build RDD with transformations
    val rawRDD = sc.textFile(dataPath)
    val header = rawRDD.first()
    val dataRDD = rawRDD.filter(line => line != header)

    val eventsRDD = dataRDD.flatMap { line =>
      val fields = line.split(",")
      if (fields.length >= 3) Seq(fields(2)) else Seq()
    }

    val eventCounts = eventsRDD
      .map(event => (event, 1))
      .reduceByKey(_ + _)

    // Show lineage
    println("\nRDD Lineage (transformation history):")
    println(eventCounts.toDebugString)

    println("\nWhat is RDD Lineage?")
    println("- Shows how RDD was created")
    println("- Records all transformations applied")
    println("- Used for fault tolerance")
    println("- Allows Spark to recompute lost data")

    println("\nExample:")
    println("  textFile() → filter() → flatMap() → map() → reduceByKey()")
    println("  If partition is lost, Spark can replay these steps")

    println("\nLINEAGE DEMO COMPLETE\n")

    spark.stop()
  }
}
