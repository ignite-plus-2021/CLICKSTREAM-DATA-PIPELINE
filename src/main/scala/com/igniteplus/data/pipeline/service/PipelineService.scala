package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.cleanseData.CleanData.filterRemoveNull
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{CLICKSTREAM_DATASET, CLICKSTREAM_NULL_ROWS_DATASET_PATH, COLUMNS_PRIMARY_KEY_CLICKSTREAM, COLUMNS_PRIMARY_KEY_ITEM, ITEM_DATASET, ITEM_NULL_ROWS_DATASET_PATH, READ_FORMAT, SPARK_CONF, WRITE_FORMAT}
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
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


    /***************** NULL VALUE CHECKING *********************************************************** */
    val nullValueCheckClickStreamDf: DataFrame = filterRemoveNull(clickStreamDataDf, COLUMNS_PRIMARY_KEY_CLICKSTREAM, CLICKSTREAM_NULL_ROWS_DATASET_PATH, WRITE_FORMAT)
    val nullValueCheckItemDf: DataFrame = filterRemoveNull(itemDataDf, COLUMNS_PRIMARY_KEY_ITEM, ITEM_NULL_ROWS_DATASET_PATH, WRITE_FORMAT)


  }
}
