package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{COLUMNS_CHECK_NULL_DQ_CHECK, COLUMNS_PRIMARY_KEY_CLICKSTREAM, EVENT_TIMESTAMP_OPTION, SQL_URL_PROD, SQL_URL_STAGING, TABLE_NAME}
import com.igniteplus.data.pipeline.dqchecks.DqCheckMethods
import com.igniteplus.data.pipeline.service.DbService.sqlWrite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.internal.Logging

object DqCheckService extends Logging {
  def executeDqCheck()(implicit spark: SparkSession): Unit = {


    /*********************************** READING THE STAGED TABLE FROM MYSQL***********************************************************************/
    val dfReadStaged:DataFrame = DbService.sqlRead(TABLE_NAME,SQL_URL_STAGING)
    logInfo("Data read from staging table completed.")

    /*********************************** CHECK NULL VALUES***********************************************************************/
    val dfCheckNull:Boolean = DqCheckMethods.DqNullCheck(dfReadStaged,COLUMNS_CHECK_NULL_DQ_CHECK)
    logInfo("Data quality null check completed.")

    /***********************************CHECK DUPLICATE VALUES***********************************************************************/
    val dfCheckDuplicate:Boolean = DqCheckMethods.DqDuplicateCheck(dfReadStaged,COLUMNS_PRIMARY_KEY_CLICKSTREAM,EVENT_TIMESTAMP_OPTION)
    logInfo("Data quality duplicate check completed.")

    /*********************************** WRITING TO PROD TABLE IN MYSQL***********************************************************************/
    if(dfCheckNull && dfCheckDuplicate){
      sqlWrite(dfReadStaged,TABLE_NAME,SQL_URL_PROD)
      logInfo("Data write to production table complete.")
    }
  }

}
