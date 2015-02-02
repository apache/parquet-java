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
package parquet.example;

import java.util.List;

import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.TypeConverter;

/**
 * Dummy implementation for perf tests
 *
 * @author Julien Le Dem
 *
 */
public final class DummyRecordConverter extends RecordMaterializer<Object> {

  private Object a;
  private GroupConverter root;

  public DummyRecordConverter(MessageType schema) {
    this.root = (GroupConverter)schema.convertWith(new TypeConverter<Converter>() {

      @Override
      public Converter convertPrimitiveType(List<GroupType> path, PrimitiveType primitiveType) {
        return new PrimitiveConverter() {

          @Override
          public void addBinary(Binary value) {
            a = value;
          }
          @Override
          public void addBoolean(boolean value) {
            a = value;
          }
          @Override
          public void addDouble(double value) {
            a = value;
          }
          @Override
          public void addFloat(float value) {
            a = value;
          }
          @Override
          public void addInt(int value) {
            a = value;
          }
          @Override
          public void addLong(long value) {
            a = value;
          }
        };
      }

      @Override
      public Converter convertGroupType(List<GroupType> path, GroupType groupType, final List<Converter> converters) {
        return new GroupConverter() {

          public Converter getConverter(int fieldIndex) {
            return converters.get(fieldIndex);
          }

          public void start() {
            a = "start()";
          }

          public void end() {
            a = "end()";
          }

        };
      }

      @Override
      public Converter convertMessageType(MessageType messageType, List<Converter> children) {
        return convertGroupType(null, messageType, children);
      }
    });
  }

  @Override
  public Object getCurrentRecord() {
    return a;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }

}
