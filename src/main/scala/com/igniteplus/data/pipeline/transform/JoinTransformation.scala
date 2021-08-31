package com.igniteplus.data.pipeline.transform

import org.apache.spark.sql.DataFrame

object JoinTransformation {
  def joinTable(df1: DataFrame, df2: DataFrame, joinKey: String, joinType: String): DataFrame = {
    val joinedTable:DataFrame = df1.join(df2,Seq(joinKey),joinType)
    joinedTable
  }

}
