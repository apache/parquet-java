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
package parquet.hadoop.thrift;
import org.apache.hadoop.mapreduce.Job;
import org.apache.thrift.TBase;

import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.util.ContextUtil;

/**
 *
 * @author Julien Le Dem
 *
 * @param <T> the thrift class use for serialization
 */
public class ParquetThriftOutputFormat<T extends TBase<?,?>> extends ParquetOutputFormat<T> {

  public static void setThriftClass(Job job, Class<? extends TBase<?,?>> thriftClass) {
    TBaseWriteSupport.setTBaseClass(ContextUtil.getConfiguration(job), thriftClass);
  }

  public static Class<? extends TBase<?,?>> getThriftClass(Job job) {
    return TBaseWriteSupport.getTBaseClass(ContextUtil.getConfiguration(job));
  }

  public ParquetThriftOutputFormat() {
    super(new TBaseWriteSupport<T>());
  }

}
