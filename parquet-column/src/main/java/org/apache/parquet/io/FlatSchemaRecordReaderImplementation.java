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
package org.apache.parquet.io;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.parquet.Log;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.filter2.recordlevel.FilteringRecordMaterializer;

/**
 * used to read reassembled records after filtering
 * reads only the required columns for evaluating the filter
 * further optimized for db applications that use flat schema
 * most of the times.
 * assumes a flat schema of record (No nested columns)
 *
 * @author Yash Datta
 *
 * @param <T> the type of the materialized record
 */
class FlatSchemaRecordReaderImplementation<T> extends RecordReader<T> {
  private static final Log LOG = Log.getLog(FlatSchemaRecordReaderImplementation.class);
  private int filterStatesOffset;
  private int numCols; 

  private final GroupConverter recordRootConverter;
  private final FilteringRecordMaterializer<T> recordMaterializer;
  
  int[] maxDefinitionLevel;
  private ColumnReader[] columnReaders;

  private boolean shouldSkipCurrentRecord = false;

  /**
   * @param root the root of the schema
   * @param recordMaterializer responsible of materializing the records
   * @param validating whether we should validate against the schema
   * @param columnStore where to read the column data from
   * @param fColsPath all columns part of the filter
   */
  public FlatSchemaRecordReaderImplementation(MessageColumnIO root, FilteringRecordMaterializer<T> recordMaterializer, boolean validating, ColumnReadStoreImpl columnStore, HashSet<String> fColsPath) {
    this.filterStatesOffset = fColsPath.size();
    this.recordMaterializer = recordMaterializer;
    this.recordRootConverter = recordMaterializer.getRootConverter(); // TODO: validator(wrap(recordMaterializer), validating, root.getType());
    PrimitiveColumnIO[] leaves = root.getLeaves().toArray(new PrimitiveColumnIO[root.getLeaves().size()]);
    numCols = leaves.length;
    columnReaders = new ColumnReader[leaves.length];
    int[] order = new int[leaves.length];
    maxDefinitionLevel = new int[leaves.length];
    for(int i=0;i < leaves.length; i++)
        order[i] = i;
    int temp = 0;
    // set the order to access the columns!
    for (int i = 0; i < leaves.length; i++ ) {
      PrimitiveColumnIO leafColumnIO = leaves[i];
      String[] path = leafColumnIO.getColumnDescriptor().getPath();
      // path[0] will give the column name
      if(fColsPath.contains(path[0])){
        int pCtemp = order[temp];
        order[temp] = order[i];
        order[i] = pCtemp;
        ++temp;
      }
    }

    for(int i = 0; i < leaves.length; i++) {
      PrimitiveColumnIO leafColumnIO = leaves[order[i]];
      maxDefinitionLevel[i] = leafColumnIO.getDefinitionLevel();
      columnReaders[i] = columnStore.getColumnReader(leafColumnIO.getColumnDescriptor());
    }
  }

  //TODO: have those wrappers for a converter
  private RecordConsumer validator(RecordConsumer recordConsumer, boolean validating, MessageType schema) {
    return validating ? new ValidatingRecordConsumer(recordConsumer, schema) : recordConsumer;
  }

  private RecordConsumer wrap(RecordConsumer recordConsumer) {
    if (Log.DEBUG) {
      return new RecordConsumerLoggingWrapper(recordConsumer);
    }
    return recordConsumer;
  }

  /**
   * @see org.apache.parquet.io.RecordReader#read()
   */
  @Override
  public T read() {
    recordRootConverter.start();
    // read all columns part of the filter first
    for(int i = 0; i < filterStatesOffset; i++) {
        ColumnReader columnReader = columnReaders[i];
        int d = columnReader.getCurrentDefinitionLevel();
        int m = maxDefinitionLevel[i];

        if (d >= m) {
          // not null
          columnReader.writeCurrentValueToConverter();
        }
        columnReader.consume();    
    }
    
    // evaluate the filter
    if(!recordMaterializer.getFilterResult()) {
      // row is rejected, skip the rest of the read
      for(int i = filterStatesOffset; i < numCols; i++) {
        ColumnReader columnReader = columnReaders[i];
        int d = columnReader.getCurrentDefinitionLevel();
        int m = maxDefinitionLevel[i];
        if (d >= m) {
          // not null
          columnReader.skip();
        }
        columnReader.consume();    
      }
      
      recordRootConverter.end(); 
      shouldSkipCurrentRecord = true;
      recordMaterializer.skipCurrentRecord();
      // signal a skip
      return null;
    }
    
    // filter passes the row, need to assemble the complete row    
    for(int i = filterStatesOffset; i < numCols; i++) {
      ColumnReader columnReader = columnReaders[i];
      int d = columnReader.getCurrentDefinitionLevel();
      int m = maxDefinitionLevel[i];
      if (d >= m) {
            // not null
        columnReader.writeCurrentValueToConverter();
       }
       columnReader.consume();    
    }
     
    recordRootConverter.end();
    shouldSkipCurrentRecord = false;
     
    return recordMaterializer.getFilteredCurrentRecord();
  }

  @Override
  public boolean shouldSkipCurrentRecord() {
    return shouldSkipCurrentRecord;
  }

  private static void log(String string) {
    LOG.debug(string);
  }
  
  protected RecordMaterializer<T> getMaterializer() {
    return recordMaterializer;
  }

  protected Converter getRecordConsumer() {
    return recordRootConverter;
  }

  protected Iterable<ColumnReader> getColumnReaders() {
    // Converting the array to an iterable ensures that the array cannot be altered
    return Arrays.asList(columnReaders);
  }
}
