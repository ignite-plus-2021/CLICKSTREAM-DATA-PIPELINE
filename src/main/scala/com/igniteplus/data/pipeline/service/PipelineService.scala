package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.cleanse.Cleanser.{dataTypeValidation, filterRemoveNull, removeDuplicates, trimColumn}
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{CLICKSTREAM_DATASET, CLICKSTREAM_NULL_ROWS_DATASET_PATH, COLUMNS_LOWERCASE_CLICKSTREAM, COLUMNS_LOWERCASE_ITEM, COLUMNS_PRIMARY_KEY_CLICKSTREAM, COLUMNS_PRIMARY_KEY_ITEM, COLUMNS_VALID_DATATYPE_CLICKSTREAM, EVENT_TIMESTAMP_OPTION, ITEM_DATASET, ITEM_NULL_ROWS_DATASET_PATH, NEW_DATATYPE_CLICKSTREAM, READ_FORMAT, SPARK_CONF, WRITE_FORMAT}
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
import jodd.util.StringUtil.toLowerCase
import org.apache.spark.sql.DataFrame

object PipelineService
{
  def pipelineService() : Unit = {

    //IMPLICIT VALUE OF SPARK
    implicit val spark = createSparkSession(SPARK_CONF)

    /*************** READING OF CLICK-STREAM DATA ****************************************************** */
    val clickStreamDataDf: DataFrame = readFile(CLICKSTREAM_DATASET, READ_FORMAT)
    /**************** READING OF ITEM DATA ************************************************************ */
    val itemDataDf: DataFrame = readFile(ITEM_DATASET, READ_FORMAT)


    /************************** CHANGE DATATYPE *****************************************************/
    val changedDatatypeClickStreamDataDf = dataTypeValidation(clickStreamDataDf, COLUMNS_VALID_DATATYPE_CLICKSTREAM,NEW_DATATYPE_CLICKSTREAM,CLICKSTREAM_DATA_TYPE_DATASET)
    val changedDatatype = dataTypeValidation(itemDataDf, COL_DATANAME_LOGDATA, DATATYPE_LOGDATA)


    /************************** TRIM COLUMNS ********************************************************/
    val trimmedClickStreamDataDf = trimColumn(changedDatatypeClickStreamDataDf)
    val trimmedItemDf = trimColumn(changedDatatype)


    /***************** NULL VALUE CHECKING *********************************************************** */
    val nullValueCheckClickStreamDataDf: DataFrame = filterRemoveNull(trimmedClickStreamDataDf, COLUMNS_PRIMARY_KEY_CLICKSTREAM, CLICKSTREAM_NULL_ROWS_DATASET_PATH, WRITE_FORMAT)
    val nullValueCheckItemDf: DataFrame = filterRemoveNull(trimmedItemDf, COLUMNS_PRIMARY_KEY_ITEM, ITEM_NULL_ROWS_DATASET_PATH, WRITE_FORMAT)

    /**************************** DEDUPLICAION *******************************************************************/
    val dedupliactedClickStreamDataDf = removeDuplicates(nullValueCheckClickStreamDataDf,COLUMNS_PRIMARY_KEY_CLICKSTREAM,Some(EVENT_TIMESTAMP_OPTION))
    val deduplicatedItemDf = removeDuplicates(nullValueCheckItemDf,COLUMNS_PRIMARY_KEY_ITEM,None)


    /*************************** CHANGE TO LOWER CASE ***************************************************************/
    val lowerCaseClickStreamDataDf = toLowerCase(dedupliactedClickStreamDataDf,COLUMNS_LOWERCASE_CLICKSTREAM)
    val lowerCaseItemDf = toLowerCase(deduplicatedItemDf,COLUMNS_LOWERCASE_ITEM)


  }
}
