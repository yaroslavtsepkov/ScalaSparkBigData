import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{countDistinct, desc, lag, row_number, sum, window}

import scala.Console.println

object Main {
  def main(args: Array[String]): Unit = {
    //Убираем часть бреда который несет спарк
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.ERROR)

     def getDistanceKm(lat1: Double, lon1: Double, lat2: Double, lon2: Double) = {
      val latRad1 = lat1 * Math.PI / 180
      val lonRad1 = lon1 * Math.PI / 180
      val latRad2 = lat2 * Math.PI / 180
      val lonRad2 = lon2 * Math.PI / 180
      val cosLat1 = Math.cos(latRad1)
      val cosLat2 = Math.cos(latRad2)
      val sinLat1 = Math.sin(latRad1)
      val sinLat2 = Math.sin(latRad2)
      val delta = Math.abs(lonRad2 - lonRad1)
      val cosDelta = Math.cos(delta)
      val sinDelta = Math.sin(delta)
      val y = Math.sqrt(Math.pow(cosLat2 * sinDelta, 2) + Math.pow(cosLat1 * sinLat2 - sinLat1 * cosLat2 * cosDelta, 2))
      val x = sinLat1 * sinLat2 + cosLat1 * cosLat2 * cosDelta
      val ad = Math.atan2(y, x)
      (ad * 6372795) / 1000
    }

    // Создадим сессию
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Simle app")
      .getOrCreate()

    //Создами dataset trips
    val trips = spark.read.format("csv")
      .option("sep", ",")
      .option("header","true")
      .load("src/main/data/trip.csv")

    //Создим dataset stations
    val stations = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("src/main/data/station.csv")

    //Первая задача: найти велосипед с наибольшим пробегом
    println("##### first task #####")
    trips.orderBy(desc("duration")).limit(1).show()

//    println("##### second task #####")
//    val mileageofbike = trips.groupByKey(record=>record.getDouble(1)).mapValues(x=>x.getDouble(2))


    //Четвертая задача: найти количество велосипедов в системе
    println("##### fourth task #####")
    trips.agg(countDistinct("bike_id")).show()

    //Пятая задача: найти пользователей
    println("##### fifth task #####")
    trips.groupBy("id")
      .agg(sum("duration"))
      .where("sum(duration) > 180")
      .show()
  }
}
