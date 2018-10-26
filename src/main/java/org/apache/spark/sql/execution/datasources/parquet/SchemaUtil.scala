package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.types.StructType

/**
  * This class is created to let the method fromString() can be used outside org.apache.spark.sql.types namespace
  */
object SchemaUtil {
  def parseString(s : String) : StructType = {
    return StructType.fromString(s)
  }
}