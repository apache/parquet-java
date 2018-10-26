package com.uber;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.execution.datasources.parquet.SchemaUtil;

/**
 * This app depends on Parquet-1178 and Parquet-1396.
 */
public class ParquetHelloWorld {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .config("spark.sql.parquet.enableVectorizedReader", false)
                .config("parquet.crypto.encryptor.decryptor.retriever.class",
                        "org.apache.parquet.crypto.SampleFileEncDecryptorRetriever")
                .config("parquet.write.support.class",
                        org.apache.spark.sql.execution.datasources.parquet.CryptoParquetWriteSupport.class.getName())
                .getOrCreate();

        testColumnEncReadWrite(spark);
    }

    private static void testColumnEncReadWrite(SparkSession spark) {
        String schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"price\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"encrypted\": true,\"columnKeyMetaData\": \"AAA=\"}},{\"name\":\"product\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"encrypted\": false}}]}";
        StructType schema = org.apache.spark.sql.execution.datasources.parquet.SchemaUtil.parseString(schemaString);
        JavaRDD<Row> rawData =  spark.read().json("products.json").toJavaRDD();
        Dataset<Row> dataFrame = spark.createDataFrame(rawData, schema);

        dataFrame.write().mode("overwrite").parquet("file1");

        spark.read().parquet("file1").show();
    }
}