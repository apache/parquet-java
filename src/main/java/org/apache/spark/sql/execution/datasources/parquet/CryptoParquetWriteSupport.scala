package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

class CryptoParquetWriteSupport extends ParquetWriteSupport {

  override def init(configuration: Configuration): WriteContext = {
    val converter = new ParquetMetadataSchemaConverter(configuration)
    createContext(configuration, converter)
  }

  override def writeFields(
                            row: InternalRow, schema: StructType, fieldWriters: Seq[ValueWriter]): Unit = {
    //Todo add data masking fields
    super.writeFields(row, schema, fieldWriters)
  }
}