package exercises.wordcount

import org.apache.spark.sql.{Dataset, SparkSession}

object WordCountUtils {
  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      import spark.implicits._
      dataSet.flatMap(row => row.split("\\s+|,|-")).map(x => x.toLowerCase.trim) //after this each row is a single word instead of a sentence/line
    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._
      //note how reduceByKey and groupByKey work on rdd and not on dataframes/datasets
      //Also note the differences between reduceByKey and groupByKey - reduceByKey first reduces within each partition and then aggregated values are further reduced by key(and whatever)
      //function you provided resulting in much less shuffle. groupByKey causes a shuffle first to get all same keys within a single partition.
      //we are correct here and something is wrong with the test those guys wrote
      dataSet.map(x => (x,1)).rdd.reduceByKey(_+_).map(kv => s"${kv._1},${kv._2.toString}").toDS()
    }
  }
}