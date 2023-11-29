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

import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.io.RecordReaderImplementation.State;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base record reader class.
 *
 * @deprecated
 */
@Deprecated
public abstract class BaseRecordReader<T> extends RecordReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseRecordReader.class);

  public RecordConsumer recordConsumer;
  public RecordMaterializer<T> recordMaterializer;
  public ColumnReadStore columnStore;

  @Override
  public T read() {
    readOneRecord();
    return recordMaterializer.getCurrentRecord();
  }

  protected abstract void readOneRecord();

  State[] caseLookup;

  private String endField;

  private int endIndex;

  protected void currentLevel(int currentLevel) {
    LOG.debug("currentLevel: {}", currentLevel);
  }

  protected void log(String message) {
    LOG.debug("bc: {}", message);
  }

  protected final int getCaseId(int state, int currentLevel, int d, int nextR) {
    return caseLookup[state].getCase(currentLevel, d, nextR).getID();
  }

  protected final void startMessage() {
    // reset state
    endField = null;
    LOG.debug("startMessage()");
    recordConsumer.startMessage();
  }

  protected final void startGroup(String field, int index) {
    startField(field, index);
    LOG.debug("startGroup()");
    recordConsumer.startGroup();
  }

  private void startField(String field, int index) {
    LOG.debug("startField({},{})", field, index);
    if (endField != null && index == endIndex) {
      // skip the close/open tag
      endField = null;
    } else {
      if (endField != null) {
        // close the previous field
        recordConsumer.endField(endField, endIndex);
        endField = null;
      }
      recordConsumer.startField(field, index);
    }
  }

  protected final void addPrimitiveINT64(String field, int index, long value) {
    startField(field, index);
    LOG.debug("addLong({})", value);
    recordConsumer.addLong(value);
    endField(field, index);
  }

  private void endField(String field, int index) {
    LOG.debug("endField({},{})", field, index);
    if (endField != null) {
      recordConsumer.endField(endField, endIndex);
    }
    endField = field;
    endIndex = index;
  }

  protected final void addPrimitiveBINARY(String field, int index, Binary value) {
    startField(field, index);
    LOG.debug("addBinary({})", value);
    recordConsumer.addBinary(value);
    endField(field, index);
  }

  protected final void addPrimitiveINT32(String field, int index, int value) {
    startField(field, index);
    LOG.debug("addInteger({})", value);
    recordConsumer.addInteger(value);
    endField(field, index);
  }

  protected final void endGroup(String field, int index) {
    if (endField != null) {
      // close the previous field
      recordConsumer.endField(endField, endIndex);
      endField = null;
    }
    LOG.debug("endGroup()");
    recordConsumer.endGroup();
    endField(field, index);
  }

  protected final void endMessage() {
    if (endField != null) {
      // close the previous field
      recordConsumer.endField(endField, endIndex);
      endField = null;
    }
    LOG.debug("endMessage()");
    recordConsumer.endMessage();
  }

  protected void error(String message) {
    throw new ParquetDecodingException(message);
  }
}
