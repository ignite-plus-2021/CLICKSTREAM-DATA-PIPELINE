package com.igniteplus.data.pipeline.dqchecks

import com.igniteplus.data.pipeline.constants.ApplicationConstants.ROW_NUMBER
import com.igniteplus.data.pipeline.exception.{DqDuplicateCheckFail, DqNullCheckFail}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}

object DqCheckMethods {

  def DqNullCheck(df: DataFrame, keyColumns: Seq[String]): Unit = {
    var nullDf: DataFrame = df
    for (i <- keyColumns)
      nullDf = df.filter(df(i).isNull)
    if (nullDf.count() > 0)
      throw new DqNullCheckFail("The file contains nulls")
  }

  def DqDuplicateCheck (df:DataFrame, KeyColumns : Seq[String], orderByCol: String = "") : Unit  = {
    if( orderByCol.isEmpty)  {
      val dfDropDuplicate = df.dropDuplicates(KeyColumns)
      if(df.count()!=dfDropDuplicate.count())
        throw new DqDuplicateCheckFail("The file contains duplicate")
    }
    else{
      val windowSpec = Window.partitionBy(KeyColumns.map(col):_* ).orderBy(desc(orderByCol))
      val dfDropDuplicate: DataFrame = df.withColumn(colName = ROW_NUMBER, row_number().over(windowSpec))
        .filter(col(ROW_NUMBER) === 1).drop(ROW_NUMBER)
      if(df.count()!=dfDropDuplicate.count())
        throw new DqDuplicateCheckFail("The file contains duplicate")
    }
  }

}
