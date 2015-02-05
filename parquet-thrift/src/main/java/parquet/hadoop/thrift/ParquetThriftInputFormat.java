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
package parquet.hadoop.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import parquet.hadoop.ParquetInputFormat;

public class ParquetThriftInputFormat<T> extends ParquetInputFormat<T> {

  @SuppressWarnings("unchecked")
  public ParquetThriftInputFormat() {
    this(ThriftReadSupport.class);
  }

  /**
   * ScroogeReadSupport can be used when reading scrooge records out of parquet file
   * @param readSupportClass
   */
  protected ParquetThriftInputFormat(Class readSupportClass) {
    super(readSupportClass);
  }

  /**
   * Call this method when setting up your Hadoop job if reading into a Thrift object
   * that is not encoded into the parquet-serialized thrift metadata (for example,
   * writing with Apache Thrift, but reading back into Twitter Scrooge version of
   * the same thrift definition, or a different but compatible Apache Thrift class).
   * @param conf
   * @param klass
   */
  public static <T> void setThriftClass(JobConf conf, Class<T> klass) {
    conf.set(ThriftReadSupport.THRIFT_READ_CLASS_KEY, klass.getName());
  }

  /**
   * Call this method when setting up your Hadoop job if reading into a Thrift object
   * that is not encoded into the parquet-serialized thrift metadata (for example,
   * writing with Apache Thrift, but reading back into Twitter Scrooge version of
   * the same thrift definition, or a different but compatible Apache Thrift class).
   * @param conf
   * @param klass
   */
  public static  <T> void setThriftClass(Configuration conf, Class<T> klass) {
    conf.set(ThriftReadSupport.THRIFT_READ_CLASS_KEY, klass.getName());
  }
}
