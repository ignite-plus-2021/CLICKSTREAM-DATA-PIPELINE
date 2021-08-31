package com.igniteplus.data.pipeline.constants
import com.igniteplus.data.pipeline.util.ApplicationUtil.getSparkConf
import org.apache.spark.SparkConf



object ApplicationConstants {

  //SPARK_SESSION
  val SPARK_CONF_FILE_NAME = "spark.conf"
  val SPARK_CONF : SparkConf = getSparkConf(SPARK_CONF_FILE_NAME)


  //DATASET
  val CLICKSTREAM_DATASET : String = "data/Input/clickstream/clickstream_log.csv"
  val ITEM_DATASET : String = "data/Input/item/item_data.csv"
  val INPUT_NULL_CLICKSTREAM_DATA : String = "data/Output/Pipeline-failures/NullClickstreamData"
  val INPUT_NULL_ITEM_DATA : String = "data/Output/Pipeline-failures/NullItemData"

  //null values writing path
  val CLICKSTREAM_NULL_ROWS_DATASET_PATH : String ="data/output/pipeline-failures/clickstream_null_values"
  val ITEM_NULL_ROWS_DATASET_PATH : String ="data/output/pipeline-failures/item_null_values"


  //DATASET FORMAT
  val READ_FORMAT :String = "csv"
  val WRITE_FORMAT :String = "csv"

  // column name Clickstream
  val EVENT_TIMESTAMP : String = "event_timestamp"
  val SESSION_ID : String = "session_id"
  val ITEM_ID : String = "item_id"
  val VISITOR_ID : String = "visitor_id"
  val REDIRECTION_SOURCE : String = "redirection_source"

  // column name Item
  val DEPARTMENT_NAME : String = "department_name"
  val ITEM_PRICE : String = "item_price"

  //timestamp datatype and timestamp format for changing datatype
  val TIMESTAMP_DATATYPE : String = "timestamp"
  val TTIMESTAMP_FORMAT : String = "MM/dd/yyyy H:mm"


  //column for Changing DATATYPE
  val COLUMNS_VALID_DATATYPE_CLICKSTREAM : Seq[String] = Seq(ApplicationConstants.EVENT_TIMESTAMP)
  val COLUMNS_VALID_DATATYPE_ITEM : Seq[String] = Seq(ApplicationConstants.ITEM_PRICE)


  //new DATATYPE
  val NEW_DATATYPE_CLICKSTREAM :Seq[String]= Seq("timestamp")
  val NEW_DATATYPE_ITEM :Seq[String]= Seq("float")

  //Primary key
  val COLUMNS_PRIMARY_KEY_CLICKSTREAM : Seq[String] = Seq(ApplicationConstants.SESSION_ID,ApplicationConstants.ITEM_ID)
  val COLUMNS_PRIMARY_KEY_ITEM : Seq[String] = Seq(ApplicationConstants.ITEM_ID)

  //Lowercase column
  val COLUMNS_LOWERCASE_CLICKSTREAM : Seq[String] = Seq(ApplicationConstants.REDIRECTION_SOURCE)
  val COLUMNS_LOWERCASE_ITEM : Seq[String] = Seq(ApplicationConstants.DEPARTMENT_NAME)

  val FAILURE_EXIT_CODE :Int = 1
  val SUCCESS_EXIT_CODE:Int = 0

  val ROW_NUMBER :String = "row_number"
  val ROW_CONDITION : String = "row_number == 1"


  val EVENT_TIMESTAMP_OPTION :String= "event_timestamp"
  
  //join
  val JOIN_KEY : String = "item_id"
  val JOIN_TYPE_NAME : String = "left"

  //Dq Check
  val COLUMNS_CHECK_NULL_DQ_CHECK : Seq[String] = Seq(ApplicationConstants.SESSION_ID, ApplicationConstants.ITEM_ID,ApplicationConstants.VISITOR_ID)


  /** Write to SQL Database */
  val JDBC_DRIVER : String = "com.mysql.cj.jdbc.Driver"
  val USER_NAME : String = "root"
  val KEY_PASSWORD : String = "4884"

  //In your mysql workbench, make two schema ignite_staging and ignite_prod with a table name "finaldata" in both the schema
  //ignite_staging: database before dq check
  //ignite_prod: database after dq check
  //change your user and password accordingly
  val SQL_URL_STAGING : String = "jdbc:mysql://localhost:3306/ignite_staging"
  val SQL_URL_PROD : String = "jdbc:mysql://localhost:3306/ignite_prod"
  val TABLE_NAME :String ="finaldata"

  val LOCATION_SQL_PASSWORD : String = "E:\\targetDEProduct_SQLPassword.txt"
  val LOCATION_ENCRYPTED_PASSWORD : String = "credentials/SQL_password_file"
  val TABLE_CLICKSTREAM_DATA : String = "CLICKSTREAM_DATA"
  val TABLE_ITEM_DATA : String = "ITEM_DATA"
  val KEY_TYPE : String = "JCEKS"
  val KEY_LOCATION : String = "credentials/mykeystore.jks"
  val CRYPTOGRAPHY_ALGORITHM : String = "AES"
  val KEY_ALIAS : String = "mykey"

}
