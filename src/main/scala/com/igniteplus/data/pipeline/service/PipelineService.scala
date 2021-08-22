package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.cleanse.Cleanser.{dataTypeValidation, filterRemoveNull, removeDuplicates, toLowerCase, trimColumn}
import com.igniteplus.data.pipeline.constants.ApplicationConstants._
import com.igniteplus.data.pipeline.service.DbService.sqlWrite
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.transform.JoinTransformation
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.spark.sql.DataFrame

object PipelineService
{
  def executePipeline() : Unit = {

    //IMPLICIT VALUE OF SPARK
    implicit val spark = createSparkSession(SPARK_CONF)

    /*************** READING OF CLICK-STREAM DATA *******************************************************/
    val clickStreamDataDf: DataFrame = readFile(CLICKSTREAM_DATASET, READ_FORMAT).drop("id")

    /**************** READING OF ITEM DATA *************************************************************/
    val itemDataDf: DataFrame = readFile(ITEM_DATASET, READ_FORMAT)

    /************************** CHANGE DATATYPE *****************************************************/
    val changedDatatypeClickStreamDataDf = dataTypeValidation(clickStreamDataDf, COLUMNS_VALID_DATATYPE_CLICKSTREAM,NEW_DATATYPE_CLICKSTREAM)
    val changedDatatype = dataTypeValidation(itemDataDf, COLUMNS_VALID_DATATYPE_ITEM, NEW_DATATYPE_ITEM)

    /************************** TRIM COLUMNS ********************************************************/
    val trimmedClickStreamDataDf = trimColumn(changedDatatypeClickStreamDataDf)
    val trimmedItemDf = trimColumn(changedDatatype)

    /***************** NULL VALUE CHECKING ************************************************************/
    val nullValueCheckClickStreamDataDf: DataFrame = filterRemoveNull(trimmedClickStreamDataDf, COLUMNS_PRIMARY_KEY_CLICKSTREAM, CLICKSTREAM_NULL_ROWS_DATASET_PATH, WRITE_FORMAT)
    val nullValueCheckItemDf: DataFrame = filterRemoveNull(trimmedItemDf, COLUMNS_PRIMARY_KEY_ITEM, ITEM_NULL_ROWS_DATASET_PATH, WRITE_FORMAT)

    /**************************** DEDUPLICAION *******************************************************************/
    val dedupliactedClickStreamDataDf = removeDuplicates(nullValueCheckClickStreamDataDf,COLUMNS_PRIMARY_KEY_CLICKSTREAM,Some(EVENT_TIMESTAMP_OPTION))
    val deduplicatedItemDf = removeDuplicates(nullValueCheckItemDf,COLUMNS_PRIMARY_KEY_ITEM,None)

    /*************************** CHANGE TO LOWER CASE ***************************************************************/
    val lowerCaseClickStreamDataDf: DataFrame = toLowerCase(dedupliactedClickStreamDataDf,COLUMNS_LOWERCASE_CLICKSTREAM)
    val lowerCaseItemDf: DataFrame = toLowerCase(deduplicatedItemDf,COLUMNS_LOWERCASE_ITEM)
    
    /*********************************** JOIN ***********************************************************************/
    val jointDf: DataFrame = JoinTransformation.joinTable(lowerCaseClickStreamDataDf, lowerCaseItemDf, JOIN_KEY, JOIN_TYPE_NAME)
    
    /*********************************** WRITING TO STAGING TABLE***********************************************************************/
    sqlWrite(jointDf,TABLE_NAME,SQL_URL_STAGING)

  }
}
