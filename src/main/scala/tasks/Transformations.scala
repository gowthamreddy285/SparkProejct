package tasks

import org.apache.spark.sql.SparkSession

// RDD Transformations
object Transformations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Transformations")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val dataPath = "file:///Users/kireeti/Desktop/Projects/SparkProejct/data/events.csv"

    println("\nTASK 2: TRANSFORMATIONS")

    // Load data
    val rawRDD = sc.textFile(dataPath)
    val header = rawRDD.first()
    val dataRDD = rawRDD.filter(line => line != header)

    // Transformation 1: filter() - Remove empty lines
    println("\n1. filter() - Remove empty lines")
    val validRDD = dataRDD.filter(_.trim.nonEmpty)
    println(s"   Records after filter: ${validRDD.count()}")

    // Transformation 2: map() - Parse CSV lines
    println("\n2. map() - Parse each line into fields")
    val parsedRDD = validRDD.map(line => line.split(","))
    println("   Example parsed record:")
    parsedRDD.take(1).foreach(fields => println(s"   Fields: ${fields.take(5).mkString(" | ")}"))

    // Transformation 3: flatMap() - Extract event types
    println("\n3. flatMap() - Extract event types")
    val eventsRDD = validRDD.flatMap { line =>
      val fields = line.split(",")
      if (fields.length >= 3) Seq(fields(2)) else Seq()
    }
    println(s"   Total events extracted: ${eventsRDD.count()}")

    // Transformation 4: distinct() - Get unique events
    println("\n4. distinct() - Find unique event types")
    val uniqueEvents = eventsRDD.distinct()
    println("   Unique events:")
    uniqueEvents.collect().foreach(e => println(s"   - $e"))

    // Transformation 5: reduceByKey() - Count each event type
    println("\n5. reduceByKey() - Count events by type")
    val eventCounts = eventsRDD
      .map(event => (event, 1))
      .reduceByKey(_ + _)

    println("   Event counts:")
    eventCounts.collect().foreach { case (event, count) =>
      println(f"   $event%-15s: $count%,d")
    }

    println("\nTRANSFORMATIONS COMPLETE\n")

    spark.stop()
  }
}
