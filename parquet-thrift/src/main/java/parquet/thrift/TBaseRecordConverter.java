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
package parquet.thrift;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import parquet.schema.MessageType;
import parquet.thrift.struct.ThriftType.StructType;

public class TBaseRecordConverter<T extends TBase<?,?>> extends ThriftRecordConverter<T> {

  public TBaseRecordConverter(final Class<T> thriftClass, MessageType parquetSchema, StructType thriftType) {
    super(new ThriftReader<T>() {
      @Override
      public T readOneRecord(TProtocol protocol) throws TException {
        try {
          T thriftObject = thriftClass.newInstance();
          thriftObject.read(protocol);
          return thriftObject;
        } catch (InstantiationException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }, thriftClass.getSimpleName(), parquetSchema, thriftType);
  }

}
