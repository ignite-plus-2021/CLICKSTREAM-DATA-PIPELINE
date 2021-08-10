package com.igniteplus.data.pipeline.cleanseData

import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.DataFrame

object CleanData
{
  def filterRemoveNull(df : DataFrame, primaryColumns : Seq[String], filePath : String, fileFormat : String) : DataFrame = {
    var nullDf : DataFrame = df
    var notNullDf : DataFrame = df
    for( i <- primaryColumns)
    {
      nullDf = df.filter(df(i).isNull)
      notNullDf = df.filter(df(i).isNotNull)
    }
    if(nullDf.count() > 0)
      writeFile(nullDf, fileFormat, filePath)
    notNullDf
  }

}
