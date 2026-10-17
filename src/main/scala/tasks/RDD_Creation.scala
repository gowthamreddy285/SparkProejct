package tasks

import org.apache.spark.sql.SparkSession

// Create RDD from dataset
object RDD_Creation {

  def main(args: Array[String]): Unit = {

    // Start Spark
    val spark = SparkSession.builder()
      .appName("RDD_Creation")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // Path to data file
    val dataPath = "file:///Users/kireeti/Desktop/Projects/SparkProejct/data/events.csv"

    println("\nTASK 1: RDD CREATION")

    // Create RDD from CSV file
    val rawRDD = sc.textFile(dataPath)

    println("\nRDD created successfully!")
    println(s"Total lines: ${rawRDD.count()}")

    // Get header
    val header = rawRDD.first()
    println(s"\nHeader: $header")

    // Remove header to get actual data
    val dataRDD = rawRDD.filter(line => line != header)

    println(s"\nData records (without header): ${dataRDD.count()}")
    println(s"Number of partitions: ${dataRDD.getNumPartitions}")

    // Show sample record
    println("\nSample record:")
    println(dataRDD.take(1).mkString)

    println("\nRDD CREATION COMPLETE\n")

    spark.stop()
  }
}
