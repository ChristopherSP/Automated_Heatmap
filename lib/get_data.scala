//////////////////////////////////////////////
//                  Imports
//////////////////////////////////////////////

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import com.datastax.spark.connector._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.datastax.spark.connector._
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import spark.implicits._
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.{lead, lag}

//////////////////////////////////////////////
//      Initianting cassandra connection
//////////////////////////////////////////////

sc.stop
spark.stop

val conf = new SparkConf().set("spark.cassandra.connection.host", "34.196.59.158")
val sc = SparkContext.getOrCreate(conf)
val spark = SparkSession.builder.getOrCreate()
val sqlContext = SparkSession.builder().getOrCreate()

//////////////////////////////////////////////
//                  UDFs
//////////////////////////////////////////////

val getConcatenated = udf( (first: String, second: String, third: String) => { first + "-" + second + "-" + third} )

//////////////////////////////////////////////
//                Functions
//////////////////////////////////////////////

////////////////// Get data from cassandra ////////////////////
var clientg : String = null
var datetime_inig : String = null
var datetime_finalg : String = null

def consult(client: String, datetime_ini: String, datetime_final: String): DataFrame = {
    clientg = client
    datetime_inig = datetime_ini
    datetime_finalg = datetime_final
    val schemaSensor = 
    StructType(
            StructField("id_sensor",StringType)::Nil
        )
        
    val schema =
      StructType(
        StructField("id_sensor", StringType) ::
          StructField("mac_address", StringType) ::
          StructField("date_time", TimestampType) ::
          StructField("rssi", IntegerType) ::
          StructField("id_campaign", IntegerType) ::
          StructField("vendor", StringType) :: Nil
      )
      
    val mySensors = sc.cassandraTable(client,"sensor")
    
    val sensors_data = sqlContext.createDataFrame(mySensors.map(
      r => org.apache.spark.sql.Row(r.columnValues(0).toString)), schemaSensor)

    val sensors_string = sensors_data.collect.mkString(",")
    val aux = sensors_string.replace("[","'")
    val sensors = aux.replace("]","'")

    val myTable = sc.cassandraTable(client,"measurement").where("id_sensor in ("+ sensors +")  and date_time >= '" + datetime_ini + "' and date_time < '" + datetime_final + "'")
    
    val all_data = sqlContext.createDataFrame(myTable.map(
      r => org.apache.spark.sql.Row(
        r.columnValues(0).toString,
        r.columnValues(4).toString,
        new java.sql.Timestamp(r.columnValues(1).asInstanceOf[java.util.Date].getTime),
        r.columnValues(7).toString.toInt,
        r.columnValues(2).toString.toInt,
        r.columnValues(3).toString
      )), schema)
      all_data
}

/////////////// Define Filters to select clients ////////////////
object visitClassificator {
  def classificator(myTable : DataFrame): DataFrame ={
    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._
    // Diff calcs the time difference in seconds
    val threshold = 30 * 60
    //  Get a window from each mac_address
    val window = Window.partitionBy($"date_diaria",$"mac_address").orderBy("timestamp")
    // The frame ranges from the beginning (Long.MinValue) to the current row (0).
    val window2 = Window.partitionBy($"date_diaria",$"mac_address").orderBy("timestamp").rowsBetween(Long.MinValue,0)
    val window3 = Window.partitionBy($"date_diaria",$"mac_address",$"visitId").orderBy("timestamp").rowsBetween(Long.MinValue,0)
    //    Diff value between actual row and last row
    val diff = ($"timestamp").cast("long") - lag($"timestamp", 1).over(window).cast("long")
    val diffId = when($"diff".>(lit(threshold)), 1).otherwise(0)
    //  Processing Tables
    val df1 = myTable.withColumn("timestamp", $"date_time".cast("timestamp"))
    //    Create Table with diff time
    val df2 = df1.withColumn("diff", diff)
    //    Create diffID with 0 to diff < Threshold and 1 to diff > threshold
    val df3 = df2.na.fill(0).withColumn("diffId", diffId)
    //    Create visitId with each row with it's own visit id
    val dfA2 = df3.withColumn("visitId", sum(df3("diffId")).over(window2)).drop("diffId")
    //  Create permanency column with Count sum and get the permanency from each visit from each mac
    val dfA3 = dfA2.withColumn("permanency", sum(dfA2("diff")).over(window3))
    dfA3
  }
}

///////////// Define clients /////////////
def specialFilter(df:Dataset[Row]): DataFrame ={
    val permanency = 120 * 60 // Seconds
    val visits = 5
    val minTime = 30 //Seconds
    //val df2 = df.where("visitId >= " +visits).select("mac_address")
    val df2 = df.where("visitId < " +visits).select("mac_address")
    // val df3 = df.groupBy("mac_address").max("permanency").withColumnRenamed("max(permanency)","permanency").where("permanency > "+permanency+ " or permanency < "+minTime)
    val df3 = df.groupBy("mac_address").max("permanency").withColumnRenamed("max(permanency)","permanency").where("permanency < "+permanency+ " or permanency > "+minTime).drop("permanency")
    // val df4 = df2.union(df3.select("mac_address")).withColumn("count",lit(1))
    val df4 = df2.union(df3.select("mac_address"))
    //prin
    df4.distinct()
  }

//////////////////////////////////////////////
//              Define Client
//////////////////////////////////////////////

val all_data = consult("pernambucanas","2017-09-04 00:00:00","2017-09-05 00:00:00")

val all_data_all = all_data.withColumn("date_diaria",getConcatenated(year($"date_time"),month($"date_time"),dayofmonth($"date_time")))

val all_data_df = all_data_all.filter($"rssi" >= -75)

val sensorCount =  all_data_df.select("id_sensor","date_diaria").distinct().groupBy("date_diaria").count().select(mean("count")).first.get(0).toString.toFloat.toInt

var threshold = ((sensorCount * 0.6) + 1).toInt

if (threshold < 4) {
    if (sensorCount < 4)
        threshold = sensorCount - 1
    else
        threshold = 3
}

//  Filter: 2) + a MAC address detected by at least n sensors in intervals of 1 min all day long
val filter_by_interval = all_data_df.groupBy(date_format($"date_time", "yyyy/MM/dd HH:mm"),$"mac_address").agg(countDistinct($"id_sensor").as("id_sensor_count")).filter($"id_sensor_count" > threshold).groupBy($"mac_address").count().as("count")

// Cleaning Data
val resultSet1 = all_data_df.join(filter_by_interval, Seq("mac_address"), "left_outer").select("id_sensor","rssi", "date_time", "id_campaign", "mac_address", "vendor","date_diaria").where("count is not null")
val visitClass = visitClassificator.classificator(resultSet1)
val visitClass2 = visitClass.groupBy("mac_address","visitId").agg(max($"permanency")).withColumnRenamed("max(permanency)","permanency")
val filter_workers = specialFilter(visitClass2)
val resultSet2 = visitClass.join(filter_workers, Seq("mac_address")).select("id_sensor","rssi", "date_time", "id_campaign", "mac_address", "vendor","date_diaria","visitId")
resultSet2.rdd.map(x=>x.mkString(",")).coalesce(1,true).saveAsTextFile("/home/centos/" + clientg + datetime_inig.split(" ")(0) + "_" + datetime_finalg.split(" ")(0))  
