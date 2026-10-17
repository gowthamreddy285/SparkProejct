package tasks

import org.apache.spark.sql.SparkSession

// Fault Tolerance
object FaultTolerance {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FaultTolerance")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println("\nTASK 9: FAULT TOLERANCE")

    println("\nHow Spark handles failures:")
    println()
    println("1. RDD Lineage")
    println("   - Spark remembers how each RDD was created")
    println("   - Stores the transformation chain")
    println()
    println("2. Partition Loss")
    println("   - If a partition is lost (node failure)")
    println("   - Spark recomputes only that partition")
    println("   - Uses the lineage information")
    println()
    println("3. No Data Replication")
    println("   - Unlike Hadoop (3x replication)")
    println("   - Spark recomputes from source")
    println("   - Saves storage space")
    println()
    println("4. Efficient Recovery")
    println("   - Only lost partitions recomputed")
    println("   - Not the entire dataset")
    println("   - Fast recovery")

    println("\nExample:")
    println("  textFile() → filter() → map() → reduceByKey()")
    println()
    println("  If partition 2 of reduceByKey() is lost:")
    println("  1. Spark checks lineage")
    println("  2. Recomputes partition 2 only")
    println("  3. Replays: textFile → filter → map → reduceByKey")
    println("  4. Restores lost data")

    println("\nBenefits:")
    println("✓ Automatic recovery")
    println("✓ No manual intervention")
    println("✓ Efficient (recompute only what's needed)")
    println("✓ No data replication overhead")

    println("\nFAULT TOLERANCE EXPLAINED\n")

    spark.stop()
  }
}
