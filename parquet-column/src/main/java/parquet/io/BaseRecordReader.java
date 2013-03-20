/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.io;

import static parquet.Log.DEBUG;
import parquet.Log;
import parquet.column.ColumnReadStore;
import parquet.io.RecordReaderImplementation.State;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordConsumer;
import parquet.io.api.RecordMaterializer;

// TODO(julien): this class appears to be unused -- can it be nuked? - todd
public abstract class BaseRecordReader<T> extends RecordReader<T> {
  private static final Log LOG = Log.getLog(BaseRecordReader.class);

  public RecordMaterializer<T> recordMaterializer;
  public ColumnReadStore columnStore;
  @Override
  public T read() {
    readOneRecord();
    return recordMaterializer.getCurrentRecord();
  }

  protected abstract void readOneRecord();

  State[] caseLookup;

  protected void currentLevel(int currentLevel) {
    if (DEBUG) LOG.debug("currentLevel: "+currentLevel);
  }

  protected void log(String message) {
    if (DEBUG) LOG.debug("bc: "+message);
  }

  final protected int getCaseId(int state, int currentLevel, int d, int nextR) {
    return caseLookup[state].getCase(currentLevel, d, nextR).getID();
  }

  protected void error(String message) {
    throw new ParquetDecodingException(message);
  }
}