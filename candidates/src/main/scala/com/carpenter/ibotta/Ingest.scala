package com.carpenter.ibotta

import java.util.Properties

import org.apache.commons.cli.CommandLine
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

/**
	* Created by scarpenter on 4/30/17.
	*/
object Ingest {

	//desired column names for each table.
	val serviceColumnNames = Seq("case_summary",
		"case_status",
		"case_source",
		"case_created_date",
		"case_created_dttm",
		"case_closed_date",
		"case_closed_dttm",
		"first_call_resolution",
		"customer_zip_code",
		"incident_address_1",
		"incident_address_2",
		"incident_intersection_1",
		"incident_intersection_2",
		"incident_zip_code",
		"longitude",
		"latitude",
		"agency",
		"division",
		"major_area",
		"type",
		"topic",
		"council_district",
		"police_district",
		"neighborhood"
	)

	val trafficNames = Seq(
		"incident_id",
		"offense_id",
		"offense_code",
		"offense_code_extension",
		"offense_type_id",
		"offense_category_id",
		"first_occurrence_date",
		"last_occurrence_date",
		"reported_date",
		"incident_address",
		"geo_x",
		"geo_y",
		"geo_lon",
		"geo_lat",
		"district_id",
		"precinct_id",
		"neighborhood_id",
		"bicycle_ind",
		"pedestrian_ind"
	)

	//Declare sparkSession as implicit so that it need not be passed around to functions that operate on data frames and load the implicits from the sparkSession to use $"" syntax.
	implicit val sparkSession: SparkSession = SparkSession.builder().appName("Ibotta").getOrCreate()

	import sparkSession.implicits._

	//UDF to clean up zips into only 5 digit ints. This was a lot more useful in my mind before I realized traffic accidents has no zips.
	//This *could* be solved with a udf calling google maps API to add zip for address albeit slow and expensive on the API.
	//Returns -1 if zip is malformed or absent
	val fixZip: String => Int = (zip: String) => {
		if (zip.length > 5) Try(zip.substring(0, 5).toInt) match {
			case Success(x) => x
			case Failure(x) => -1
		}
		else if (zip.length == 5) Try(zip.toInt) match {
			case Success(x) => x
			case Failure(x) => -1
		}
		else if (zip == "null") -1
		else -1
	}

	val fixNeighborhood: String => String = (neighborHood: String) =>
		if (neighborHood.nonEmpty) neighborHood.split(" ").map(_.toLowerCase).mkString("-") else ""

	def main(args: Array[String]): Unit = {
		//Load cmd args
		val cmd = parseOptions(args)
		val username = cmd.getOptionValue("u")
		val password = cmd.getOptionValue("p")
		val jdbcUrl = cmd.getOptionValue("j")

		//Load the raw CSV's which unfortunately have bad column names and are all text typed
		val serviceCalls = ingestRawData("311_service_requests.csv")
		val trafficIncidents = ingestRawData("traffic_accidents.csv")

		//Rename service calls columns and properly type them before saving to postgres
		val serviceCallsWithColumnsRenamed = serviceCalls.toDF(serviceColumnNames: _*)
		val cleanServiceCalls = cleanService(serviceCallsWithColumnsRenamed)
		saveToSql(cleanServiceCalls, jdbcUrl, "service_requests", username, password)

		//Rename traffic accidents columns and properly type them before saving to postgres
		val trafficAccidentsWithColumnsRenamed = trafficIncidents.toDF(trafficNames: _*)
		val cleanTrafficAccidents = cleanTraffic(trafficAccidentsWithColumnsRenamed)
		cleanTrafficAccidents.show()
		saveToSql(cleanTrafficAccidents, jdbcUrl, "traffic_accidents", username, password)
	}

	def cleanZip: UserDefinedFunction = udf(fixZip)
	def cleanNeighborhood: UserDefinedFunction = udf(fixNeighborhood)

	//This function will cast columns with specific logic for dates and zip codes. Could be expanded and is certainly not bulletproof.
	def convertColumn(df: DataFrame, name: String, newType: String): DataFrame = {
		newType match {
				//special case because to_date isnt playing nicely with the service_requests data
			case "date" => {
				val df_1 = df.withColumnRenamed(name, "swap")
				df_1.withColumn(name, unix_timestamp($"swap", "MM/dd/yyyy").cast("timestamp")).drop("swap")
			}
			case "neighborhood" => {
				val df_1 = df.withColumnRenamed(name, "swap").na.fill("", Seq("swap"))
				df_1.withColumn(name, cleanNeighborhood($"swap")).drop("swap")
			}
			case "zip" => {
				val df_1 = df.withColumnRenamed(name, "swap")
				df_1.withColumn(name, cleanZip($"swap")).drop("swap")
			}
			case _ => {
				val df_1 = df.withColumnRenamed(name, "swap")
				df_1.withColumn(name, df_1.col("swap").cast(newType)).drop("swap")
			}
		}
	}

	//One liner to abstract away the load
	def ingestRawData(path: String)(implicit sparkSession: SparkSession): DataFrame = sparkSession.read.option("header", true).csv(path)

	//Sadly spark does not have a good way to cast columns in bulk. Due to the immutability of dataframes a new one must be created one at a time.
	//Cleaning functions for each raw df to abstract this mess away. If you have a better way to do this I would love to hear about it because it makes me sick to write this code.
	def cleanService(dataFrame: DataFrame): DataFrame = {
		val fill = dataFrame.na.fill("null", Seq("customer_zip_code", "incident_zip_code"))
		val a = convertColumn(fill, "case_created_date", "date")
		val b = convertColumn(a, "case_closed_date", "date")
		val c = convertColumn(b, "latitude", "float")
		val d = convertColumn(c, "longitude", "float")
		val e = convertColumn(d, "police_district", "int")
		val f = convertColumn(e, "council_district", "int")
		val g = convertColumn(f, "customer_zip_code", "zip")
		val h = convertColumn(g, "incident_zip_code", "zip")
		val i = convertColumn(h, "neighborhood", "neighborhood")
		i
	}

	def cleanTraffic(dataFrame: DataFrame): DataFrame = {
		val a = convertColumn(dataFrame, "incident_id", "int")
		val b = convertColumn(a, "offense_id", "int")
		val c = convertColumn(b, "offense_code", "int")
		val d = convertColumn(c, "offense_code_extension", "int")
		val e = convertColumn(d, "geo_lat", "float")
		val f = convertColumn(e, "geo_lon", "float")
		val g = convertColumn(f, "district_id", "int")
		val h = convertColumn(g, "precinct_id", "int")
		val i = convertColumn(h, "geo_x", "int")
		val j = convertColumn(i, "geo_y", "int")
		val k = convertColumn(j, "bicycle_ind", "int")
		val l = convertColumn(k, "pedestrian_ind", "int")
		l.withColumn("first_occurrence_date_cast", to_date($"first_occurrence_date"))
			.withColumn("last_occurrence_date_cast", to_date($"last_occurrence_date"))
			.withColumn("reported_date_cast", to_date($"reported_date"))
	}

	//Abstract away the save. Must set up properties within the function to ensure that they are instantiated on all executors.
	def saveToSql(dataFrame: DataFrame, databaseUrl: String, table: String, username: String, password: String): Unit = {
		val prop = new Properties()
		prop.setProperty("user", username)
		prop.setProperty("password", password)
		prop.setProperty("driver", "org.postgresql.Driver")
		dataFrame.write.mode(SaveMode.Append).jdbc(databaseUrl, table, prop)
	}

	//Command line parser
	def parseOptions(args: Array[String]): CommandLine = {
		import org.apache.commons.cli.{GnuParser, Option, Options}
		new GnuParser().parse(new Options()
			.addOption(new Option("u", true, "username"))
			.addOption(new Option("p", true, "password"))
			.addOption(new Option("j", true, "jdbc")), args)
	}
}