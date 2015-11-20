import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.cassandra.CassandraSQLContext


object RollupRetailDataframe {

  def main(args: Array[String]) {

//    Create Spark Context
    val conf = new SparkConf(true).setAppName("RollupRetailHiveQL")

// We set master on the command line for flexibility

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val csc = new CassandraSQLContext(sc)

    // Nice handy dandy function.  It picks up the current value of SQLContext at execution
    // so it's breaks encapsulation

    def cassandra_df (ks:String, table:String) =  sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"-> ks, "table" -> table))
      .load()

    val receipts_by_store_date_df = cassandra_df("retail","receipts_by_store_date")
    val stores_df = cassandra_df("retail","stores")

    // Create some handy UDF's

    val concat = udf((s1:String, s2:String) => s1 + s2)

    // Create Dataframe to get sales by state

    val sales_by_state_df = receipts_by_store_date_df
      .join(stores_df, stores_df("store_id") === receipts_by_store_date_df("store_id"))
      .groupBy(stores_df("state"))
      .sum("receipt_total")
      .select(lit("dummy") alias "dummy", col("state"), concat( lit("US-"), col("state")) alias "region", col("SUM(receipt_total)") cast "Decimal(10,2)" alias ("receipts_total"))

    sales_by_state_df.write                         // Save the dataframe.
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "retail",
                  "table" -> "sales_by_state"))
      .mode(SaveMode.Overwrite)
      .save()


    // Compute Sales by date

    val sales_by_date_df = receipts_by_store_date_df
     .groupBy("receipt_date")
      .sum("receipt_total")
      .select(lit("dummy") alias "dummy", col("receipt_date") as "sales_date", col("SUM(receipt_total)") cast "Decimal(10,2)" alias "receipts_total")

    sales_by_date_df.write                         // Save the dataframe.
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "retail",
      "table" -> "sales_by_date"))
      .mode(SaveMode.Overwrite)
      .save()

    csc.setKeyspace("retail")
    csc.sql("select rc.credit_card_number, rs.state from retail.stores as rs join  retail.receipts_by_credit_card as rc on  rs.store_id =  rc.store_id group by rc.credit_card_number, rs.state ")
      .registerTempTable("cc_by_state")
    val fraudDf = csc.sql("select s1. credit_card_number as credit_card_number, s1.state as state, s2.state as susp_state from cc_by_state as s1 join cc_by_state as s2 on s1.credit_card_number = s2.credit_card_number and s1.state > s2.state")

    fraudDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "retail", "table" -> "fraud_activities"))
      .mode(SaveMode.Overwrite)
      .save()

  }
}

