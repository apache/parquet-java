package org.apache.spark.sql.execution.datasources.parquet

import java.util
import java.util.Map

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.{ExtType, MessageType, Type, Types}
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

import scala.collection.JavaConversions._

/**
  * This class pass field's metadata in StructType to the field of MessageType in addition to
  * existing functions of ParquetSchemaConverter.
  *
  * It has dependency on the class ExtType defined in the link below. Parquet-1396 is opened to merge
  * ExtType to Parquet-mr repo. https://github.com/shangxinli/parquet-mr/blob/encryption/parquet-column/
  * src/main/java/org/apache/parquet/schema/ExtType.java
  *
  */
class ParquetMetadataSchemaConverter(conf: Configuration) extends ParquetSchemaConverter(conf) {

    /**
    * Converts a Spark SQL [[StructField]] to a Parquet [[Type]].
    */
  override def convert(catalystSchema: StructType): MessageType = {
    Types
      .buildMessage()
      .addFields(catalystSchema.map(convertFieldWithMetadata): _*)
      .named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
  }

  def convertFieldWithMetadata(field: StructField) : Type = {
    val extField  = new ExtType[Any](convertField(field))
    val metaBuilder = new ExtMetadataBuilder().withMetadata(field.metadata)
    val metaData = metaBuilder.getMap
    extField.setMetadata(metaData)
    return extField
  }

  private def getMetadata(schema : StructType , fieldName : String) : Map[String, Any] = {
    schema.fields.foreach{ field =>
      if (field.name != null && field.name.equals(fieldName)) {
        val metaBuilder = new ExtMetadataBuilder().withMetadata(field.metadata)
        return metaBuilder.getMap
      }
    }
    return new util.HashMap[String, Any]()
  }
}

/**
  * Due to the access modifier of getMap() in Spark, ExtMetadataBuilder is created to let getMap can be
  * accessed in above class.
  */
class ExtMetadataBuilder extends MetadataBuilder {

  override def getMap = {
    super.getMap
  }
}
