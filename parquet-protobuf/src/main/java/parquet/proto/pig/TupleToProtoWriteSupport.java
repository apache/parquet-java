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
package parquet.proto.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import parquet.hadoop.BadConfigurationException;
import parquet.hadoop.api.WriteSupport;
import parquet.proto.ProtoWriteSupport;
import parquet.io.api.RecordConsumer;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.twitter.elephantbird.pig.util.PigToProtobuf;
import com.twitter.elephantbird.util.Protobufs;

/**
 * Stores Pig tuples as Protobuf objects
 *
 * @author Peter Lin
 *
 */
public class TupleToProtoWriteSupport extends WriteSupport<Tuple> {

  private final String className;
  private ProtoWriteSupport<MessageOrBuilder> protoWriteSupport;
  private Class<? extends Message> protoClass;

  /**
   * @param className the thrift class name
   */
  public TupleToProtoWriteSupport(String className) {
    super();
    this.className = className;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public WriteContext init(Configuration configuration) {
    try {
      protoClass = Protobufs.getInnerProtobufClass(className);
      protoWriteSupport = new ProtoWriteSupport(protoClass);
      return protoWriteSupport.init(configuration);      
    } catch (ClassCastException e) {
      throw new BadConfigurationException("The protobuf class name should extend Message: " + className, e);
    }
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    protoWriteSupport.prepareForWrite(recordConsumer);
  }

  @Override
  public void write(Tuple t) {
    protoWriteSupport.write(PigToProtobuf.tupleToMessage(protoClass, t));
  }

}
