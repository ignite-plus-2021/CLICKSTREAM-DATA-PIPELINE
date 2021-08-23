package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{COLUMNS_CHECK_NULL_DQ_CHECK, COLUMNS_PRIMARY_KEY_CLICKSTREAM, EVENT_TIMESTAMP_OPTION, SQL_URL_PROD, SQL_URL_STAGING, TABLE_NAME}
import com.igniteplus.data.pipeline.dqchecks.DqCheckMethods
import com.igniteplus.data.pipeline.service.DbService.sqlWrite
import org.apache.spark.sql.{DataFrame, SparkSession}

object DqCheckService {
  def executeDqCheck()(implicit spark: SparkSession): Unit = {


    /*********************************** READING THE STAGED TABLE FROM MYSQL***********************************************************************/
    val dfReadStaged:DataFrame = DbService.sqlRead(TABLE_NAME,SQL_URL_STAGING)

    /*********************************** CHECK NULL VALUES***********************************************************************/
    val dfCheckNull:Boolean = DqCheckMethods.DqNullCheck(dfReadStaged,COLUMNS_CHECK_NULL_DQ_CHECK)


    /***********************************CHECK DUPLICATE VALUES***********************************************************************/
    val dfCheckDuplicate:Boolean = DqCheckMethods.DqDuplicateCheck(dfReadStaged,COLUMNS_PRIMARY_KEY_CLICKSTREAM,EVENT_TIMESTAMP_OPTION)

    /*********************************** WRITING TO PROD TABLE IN MYSQL***********************************************************************/
    if(dfCheckNull && dfCheckDuplicate){
      sqlWrite(dfReadStaged,TABLE_NAME,SQL_URL_PROD)
    }
  }

}
