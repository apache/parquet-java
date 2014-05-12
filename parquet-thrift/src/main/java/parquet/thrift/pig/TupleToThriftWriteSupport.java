/**
 * Copyright 2014 Twitter, Inc.
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
package parquet.thrift.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import parquet.hadoop.BadConfigurationException;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.thrift.TBaseWriteSupport;
import parquet.io.api.RecordConsumer;

import com.twitter.elephantbird.pig.util.PigToThrift;

/**
 * Stores Pig tuples as Thrift objects
 *
 * @author Julien Le Dem
 *
 */
public class TupleToThriftWriteSupport extends WriteSupport<Tuple> {

  private final String className;
  private TBaseWriteSupport<TBase<?,?>> thriftWriteSupport;
  private PigToThrift<TBase<?,?>> pigToThrift;

  /**
   * @param className the thrift class name
   */
  public TupleToThriftWriteSupport(String className) {
    super();
    this.className = className;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public WriteContext init(Configuration configuration) {
    try {
      Class<?> clazz = configuration.getClassByName(className).asSubclass(TBase.class);
      thriftWriteSupport = new TBaseWriteSupport(clazz);
      pigToThrift = new PigToThrift(clazz);
      return thriftWriteSupport.init(configuration);
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("The thrift class name was not found: " + className, e);
    } catch (ClassCastException e) {
      throw new BadConfigurationException("The thrift class name should extend TBase: " + className, e);
    }
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    thriftWriteSupport.prepareForWrite(recordConsumer);
  }

  @Override
  public void write(Tuple t) {
    thriftWriteSupport.write(pigToThrift.getThriftObject(t));
  }

}
