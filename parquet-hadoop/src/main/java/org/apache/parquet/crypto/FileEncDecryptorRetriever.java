/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;

import java.io.IOException;

/**
 * This interface defines Parquet file encryptor and decryptor retriever defined in
 * https://issues.apache.org/jira/browse/PARQUET-1325.
 *
 * This is for the non-API usage of Parquet crypto feature developed in
 * https://issues.apache.org/jira/browse/PARQUET-1178. The usage is described
 * as below.
 *
 * 1  Write a class to implement FileEncDecryptorRetriever. The sample implementation can be
 *    found https://github.com/shangxinli/parquetcrytosampleretriever 
 *    1.1) To implement getFileEncryptor(), you need to create an instance of
 *       ParquetFileEncryptor and return it. To create that instance, you usually need
 *       to know each column of current schema of current writing
 *       and it's encryption properties like key, key metadata. These information usually
 *       are stored outside current host and need to be retrieved. It is this class's
 *       responsibility to retrieve all the needed information to be build up ParquetFileEncryptor.
 *       But to retrieve them, it need to know the reference like ID of current schema. #3 below
 *       describe how to pass the schema reference in configuration which is send back to
 *       getFileEncryptor()
 *
 *    1.2) To implement getFileDecryptor(), you need to create an instance of
 *       ParquetFileDecryptor and return it. To create that instance, you need to implement a
 *       class to implement DecryptionKeyRetriever and create an instance of that class. As
 *       metadata are embedded in the Parquet file, key retriever is the only thing needed.
 *       The key retriever's method getKey() will be called by Parquet library during reading.
 *
 * 2  Set configuration of "parquet.crypto.encryptor.decryptor.retriever.class"
 *    with the full namespace of this class. For example, we can set SparkSession as
 *    below.
 *
 *    SparkSession spark = SparkSession
 *                 .config("parquet.crypto.encryptor.decryptor.retriever.class",
 *                         "org.apache.parquet.crypto.SampleFileEncDecryptorRetriever")
 *
 *    This class will be invoked by parquet library can call getFileEncryptor() or
 *    getFileDecryptor() inside which you can create ParquetFileEncryptor or ParquetFileDecryptor.
 *
 * 3  For every write or read, set configuration to indicate the identity of
 *    current schema. For example, you can set as below.
 *
 *    spark.sparkContext().hadoopConfiguration().set("schemaRef", "database1.table1");
 *
 *    Here, you can use other names too like "schemaId", "Id"... as long as it doesn't conflict
 *    with others in current configuration. You class that implementation this interface
 *    will need to get it back to reference current schema to create ParquetFileEncryptor or
 *    ParquetFileDecryptor.
 */

public interface FileEncDecryptorRetriever {

  /**
   * Get the file encryptor for encrypting parquet files.
   *
   * @param conf Configuration of running task
   */
  InternalFileEncryptor getFileEncryptor(Configuration conf, WriteContext writeContext) throws IOException;

  /**
   * Get the file encryption properties for encrypting parquet files.
   *
   * @param conf Configuration of running task
   */
  FileEncryptionProperties getFileEncryptionProperties(Configuration conf, WriteContext writeContext) throws IOException;


  /**
   * Get the file decryptor for decrypting parquet files.
   *
   * @param conf Configuration of running task
   */
  InternalFileDecryptor getFileDecryptor(Configuration conf) throws IOException;


  /**
   * Get the file decryption properties for encrypting parquet files.
   *
   * @param conf Configuration of running task
   */
  FileDecryptionProperties getFileDecryptionProperties(Configuration conf) throws IOException;


  enum CryptoMode {
    /**
     * There is no encryption for footer or columns.
     */
    NONE,
    /**
     * Footer and all the columns are encrypted with same key.
     */
    UNIFORM,
    /**
     * Each column and footer can be encrypted with different key or not encrypted at all.
     */
    COLUMN
  }
}
