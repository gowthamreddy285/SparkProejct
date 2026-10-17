package tasks

import org.apache.spark.sql.SparkSession

// RDD Actions
object Actions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Actions")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val dataPath = "file:///Users/kireeti/Desktop/Projects/SparkProejct/data/events.csv"

    println("\nTASK 3: ACTIONS")

    // Prepare data
    val rawRDD = sc.textFile(dataPath)
    val header = rawRDD.first()
    val dataRDD = rawRDD.filter(line => line != header && line.trim.nonEmpty)

    // Get event counts
    val eventsRDD = dataRDD.flatMap { line =>
      val fields = line.split(",")
      if (fields.length >= 3) Seq(fields(2)) else Seq()
    }

    val eventCounts = eventsRDD
      .map(event => (event, 1))
      .reduceByKey(_ + _)

    // Action 1: collect() - Get all results
    println("\n1. collect() - Get all event counts")
    val results = eventCounts.collect()
    results.foreach { case (event, count) =>
      println(f"   $event%-15s: $count%,d")
    }

    // Action 2: count() - Count records
    println("\n2. count() - Total number of records")
    val totalRecords = dataRDD.count()
    println(f"   Total: $totalRecords%,d records")

    // Action 3: take() - Get first few records
    println("\n3. take() - Get first 3 sample records")
    dataRDD.take(3).zipWithIndex.foreach { case (line, i) =>
      val fields = line.split(",")
      println(s"   Record ${i+1}: ${fields.take(4).mkString(" | ")}")
    }

    // Action 4: reduce() - Sum all event counts
    println("\n4. reduce() - Sum all counts")
    val totalEvents = eventCounts
      .map { case (_, count) => count }
      .reduce(_ + _)
    println(f"   Total events: $totalEvents%,d")

    println("\nACTIONS COMPLETE\n")

    spark.stop()
  }
}
