package tasks

import org.apache.spark.sql.SparkSession

// Key-Value RDD Operations
object KeyValue {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KeyValue")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val dataPath = "file:///Users/kireeti/Desktop/Projects/SparkProejct/data/events.csv"

    println("\nTASK 4: KEY-VALUE RDD OPERATIONS")

    // Load data
    val rawRDD = sc.textFile(dataPath)
    val header = rawRDD.first()
    val dataRDD = rawRDD.filter(line => line != header && line.trim.nonEmpty)

    // Key-Value 1: Count events by type
    println("\n1. Events by type (event_type, count)")
    val eventsRDD = dataRDD.map { line =>
      val fields = line.split(",")
      fields(2) // event column
    }

    val eventCounts = eventsRDD
      .map(event => (event, 1))     // Create (key, value) pairs
      .reduceByKey(_ + _)            // Sum by key

    eventCounts.collect().foreach { case (event, count) =>
      println(f"   ($event%-15s, $count%,d)")
    }

    // Key-Value 2: Count events per visitor
    println("\n2. Events by visitor (visitor_id, count)")
    val visitorCounts = dataRDD
      .map { line =>
        val fields = line.split(",")
        (fields(1).toInt, 1)         // (visitorid, 1)
      }
      .reduceByKey(_ + _)            // Count per visitor

    println(s"   Total unique visitors: ${visitorCounts.count()}")
    println("\n   Sample visitor counts:")
    visitorCounts.take(5).foreach { case (visitor, count) =>
      println(f"   (Visitor $visitor%8d, $count%2d events)")
    }

    println("\nKEY-VALUE OPERATIONS COMPLETE\n")

    spark.stop()
  }
}
