package com.steckytech.maxwell.spark

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object JdbcHelper {

  val dbUser = "db_admin"
  val dbPass = ""

  def table(session:SparkSession, schema:String, table:String, partitions:Int): DataFrame = {
    session.read
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .jdbc("jdbc:mysql://mysql.cyberdyne:3306/"+ schema +"?serverTimezone=US/Mountain&autoReconnect=true", table, dbConnectionProperties(dbUser, dbPass))
  }

  def query(session:SparkSession, schema:String, query:String, partitions:Int): DataFrame = {
    session.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://mysql.cyberdyne:3306/"+ schema +"?serverTimezone=US/Mountain&autoReconnect=true")
      .option("user", dbUser)
      .option("password", dbPass)
      .option("query", query)
      .load()
  }

  def dbConnectionProperties(user:String, pass:String): Properties = {
    val settings = new Properties()
    settings.setProperty("user", user)
    settings.setProperty("password", pass)
    settings
  }
}
