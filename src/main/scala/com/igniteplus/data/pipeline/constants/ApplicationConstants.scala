package com.igniteplus.data.pipeline.constants
import com.igniteplus.data.pipeline.util.ApplicationUtil.{createSparkSession, getSparkConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object ApplicationConstants {

  //SPARK_SESSION
  val SPARK_CONF_FILE_NAME = "spark.conf"
  val SPARK_CONF: SparkConf = getSparkConf(SPARK_CONF_FILE_NAME)
  implicit val spark: SparkSession = createSparkSession(SPARK_CONF)

  //DATASET
  val CLICKSTREAM_DATASET: String = "data/Input/clickstream/clickstream_log.csv"
  val ITEM_DATASET: String = "data/Input/item/item_data.csv"
  val INPUT_NULL_CLICKSTREAM_DATA : String = "data/Output/Pipeline-failures/NullClickstreamData"
  val INPUT_NULL_ITEM_DATA : String = "data/Output/Pipeline-failures/NullItemData"

  //null values writing path
  val CLICKSTREAM_NULL_ROWS_DATASET_PATH: String ="data/output/pipeline-failures/clickstream_null_values"
  val ITEM_NULL_ROWS_DATASET_PATH: String ="data/output/pipeline-failures/item_null_values"


  //DATASET FORMAT
  val READ_FORMAT:String = "csv"
  val WRITE_FORMAT:String = "csv"

  // column name Clickstream
  val EVENT_TIMESTAMP: String = "event_timestamp"
  val SESSION_ID: String = "session_id"
  val ITEM_ID: String = "item_id"
  val REDIRECTION_SOURCE: String = "redirection_source"

  // column name Item
  val DEPARTMENT_NAME: String = "department_name"
  val ITEM_PRICE: String = "item_price"

  //timestamp datatype and timestamp format for changing datatype
  val TIMESTAMP_DATATYPE: String = "timestamp"
  val TTIMESTAMP_FORMAT: String = "MM/dd/yyyy H:mm"


  //column for Changing DATATYPE
  val COLUMNS_VALID_DATATYPE_CLICKSTREAM: Seq[String] = Seq(ApplicationConstants.EVENT_TIMESTAMP)
  val COLUMNS_VALID_DATATYPE_ITEM: Seq[String] = Seq(ApplicationConstants.ITEM_PRICE)


  //new DATATYPE
  val NEW_DATATYPE_CLICKSTREAM:Seq[String]= Seq("timestamp")
  val NEW_DATATYPE_ITEM:Seq[String]= Seq("float")

  //Primary key
  val COLUMNS_PRIMARY_KEY_CLICKSTREAM: Seq[String] = Seq(ApplicationConstants.SESSION_ID,ApplicationConstants.ITEM_ID)
  val COLUMNS_PRIMARY_KEY_ITEM: Seq[String] = Seq(ApplicationConstants.ITEM_ID)

  //Lowercase column
  val COLUMNS_LOWERCASE_CLICKSTREAM: Seq[String] = Seq(ApplicationConstants.REDIRECTION_SOURCE)
  val COLUMNS_LOWERCASE_ITEM: Seq[String] = Seq(ApplicationConstants.DEPARTMENT_NAME)

  val FAILURE_EXIT_CODE:Int = 1

  val ROW_NUMBER:String = "row_number"


  val EVENT_TIMESTAMP_OPTION:String= "event_timestamp"

}
