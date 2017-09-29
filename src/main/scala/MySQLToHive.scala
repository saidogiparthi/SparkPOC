import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object MySQLToHive {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    val jdbcDatabase ="sparkPOC"
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    val connectionProperties = new Properties()
    connectionProperties.put("user", "<mysql username>")
    connectionProperties.put("password", "<mysql password>")
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    val jdbcDF = spark.read.jdbc(jdbcUrl,"<tablename>", connectionProperties)

    // If you want to overwrite entire table put SaveMode.Overwrite
    jdbcDF.write.mode(SaveMode.Append).saveAsTable("default.source")

    spark.stop()
  }
}
