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
package parquet.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.mapreduce.Job;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.util.ContextUtil;

/**
 * A Hadoop {@link org.apache.hadoop.mapreduce.InputFormat} for Parquet files.
 */
public class AvroParquetInputFormat extends ParquetInputFormat<IndexedRecord> {
  public AvroParquetInputFormat() {
    super(AvroReadSupport.class);
  }

  public static void setRequestedProjection(Job job, Schema requestedProjection) {
    AvroReadSupport.setRequestedProjection(ContextUtil.getConfiguration(job), requestedProjection);
  }

}
