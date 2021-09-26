package exercises.citibike

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions.udf

//todo: Better names
object CitibikeTransformerUtils {
  private final val MetersPerFoot = 0.3048
  private final val FeetPerMile = 5280

  final val EarthRadiusInM: Double = 6371e3 //R in doc
  final val MetersPerMile: Double = MetersPerFoot * FeetPerMile

  implicit class StringDataset(val dataSet: Dataset[Row]) {

    def computeDistances(spark: SparkSession) = {
      import spark.implicits._

      val getDistanceMetresUDF = udf[Double, Double, Double, Double, Double](getDistanceMetres)
      val resultDataSet = dataSet.withColumn("distance", getDistanceMetresUDF($"start_station_latitude", $"end_station_latitude", $"start_station_longitude", $"end_station_longitude"))

      resultDataSet
    }
  }

  private def getPhi1(startLat: Double): Double = {
    0.01d
  }

  private def getPhi2(endLat: Double): Double = {
    0.01d
  }

  private def getDeltaPhi(startLat: Double, endLat: Double): Double = {
    0.01d
  }

  private def getDeltaLambda(startLong: Double, endLong: Double): Double = {
    0.01d
  }

  private def getA(phi1: Double, phi2: Double, deltaPhi: Double, deltaLambda: Double): Double = {
    0.01d
  }

  private def getC(a: Double): Double = {
    0.01d
  }

  private def getDistanceMetres(startLat: Double, endLat: Double, startLong: Double, endLong: Double): Double = {
    val phi1 = getPhi1(startLat)
    val phi2 = getPhi2(endLat)
    val deltaPhi = getDeltaPhi(startLat, endLat)
    val deltaLambda = getDeltaLambda(startLong, endLong)
    val a = getA(phi1, phi2, deltaPhi, deltaLambda)
    val c = getC(a)

    EarthRadiusInM * c
  }
}
