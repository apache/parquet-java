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
package org.apache.hadoop.hive.ql.io.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.ql.io.parquet.write.ParquetRecordWriterWrapper;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.Progressable;

import org.apache.parquet.hadoop.ParquetOutputFormat;

/**
 *
 * A Parquet OutputFormat for Hive (with the deprecated package mapred)
 *
 */
public class MapredParquetOutputFormat extends FileOutputFormat<Void, ArrayWritable> implements
  HiveOutputFormat<Void, ArrayWritable> {

  private static final Log LOG = LogFactory.getLog(MapredParquetOutputFormat.class);

  protected ParquetOutputFormat<ArrayWritable> realOutputFormat;

  public MapredParquetOutputFormat() {
    realOutputFormat = new ParquetOutputFormat<ArrayWritable>(new DataWritableWriteSupport());
  }

  public MapredParquetOutputFormat(final OutputFormat<Void, ArrayWritable> mapreduceOutputFormat) {
    realOutputFormat = (ParquetOutputFormat<ArrayWritable>) mapreduceOutputFormat;
  }

  @Override
  public void checkOutputSpecs(final FileSystem ignored, final JobConf job) throws IOException {
    realOutputFormat.checkOutputSpecs(ShimLoader.getHadoopShims().getHCatShim().createJobContext(job, null));
  }

  @Override
  public RecordWriter<Void, ArrayWritable> getRecordWriter(
      final FileSystem ignored,
      final JobConf job,
      final String name,
      final Progressable progress
      ) throws IOException {
    throw new RuntimeException("Should never be used");
  }

  /**
   *
   * Create the parquet schema from the hive schema, and return the RecordWriterWrapper which
   * contains the real output format
   */
  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(
      final JobConf jobConf,
      final Path finalOutPath,
      final Class<? extends Writable> valueClass,
      final boolean isCompressed,
      final Properties tableProperties,
      final Progressable progress) throws IOException {

    LOG.info("creating new record writer..." + this);

    final String columnNameProperty = tableProperties.getProperty(IOConstants.COLUMNS);
    final String columnTypeProperty = tableProperties.getProperty(IOConstants.COLUMNS_TYPES);
    List<String> columnNames;
    List<TypeInfo> columnTypes;

    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }

    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    DataWritableWriteSupport.setSchema(HiveSchemaConverter.convert(columnNames, columnTypes), jobConf);
    return getParquerRecordWriterWrapper(realOutputFormat, jobConf, finalOutPath.toString(), progress);
  }

  protected ParquetRecordWriterWrapper getParquerRecordWriterWrapper(
      ParquetOutputFormat<ArrayWritable> realOutputFormat,
      JobConf jobConf,
      String finalOutPath,
      Progressable progress
      ) throws IOException {
    return new ParquetRecordWriterWrapper(realOutputFormat, jobConf, finalOutPath.toString(), progress);
  }
}
