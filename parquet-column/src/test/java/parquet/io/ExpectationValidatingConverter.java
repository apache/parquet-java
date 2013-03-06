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

import static org.junit.Assert.assertEquals;

import java.util.Deque;
import java.util.List;

import parquet.io.convert.Converter;
import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.io.convert.RecordConverter;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import parquet.schema.TypeConverter;

public class ExpectationValidatingConverter extends RecordConverter<Void> {

  private GroupConverter root;

  private final Deque<String> expectations;
  int count = 0;

  public void validate(String got) {
    assertEquals("event #"+count, expectations.pop(), got);
    ++count;
  }

  public ExpectationValidatingConverter(Deque<String> expectations, MessageType schema) {
    this.expectations = expectations;
    this.root = (GroupConverter)schema.convertWith(new TypeConverter<Converter>() {

      @Override
      public Converter convertPrimitiveType(final List<GroupType> path, final PrimitiveType primitiveType) {
        return new PrimitiveConverter() {

          private void validate(String message) {
            ExpectationValidatingConverter.this.validate(path(path, primitiveType) + message);
          }

          @Override
          public void addBinary(Binary value) {
            validate("addBinary("+value.toStringUsingUTF8()+")");
          }

          @Override
          public void addBoolean(boolean value) {
            validate("addBoolean("+value+")");
          }

          @Override
          public void addDouble(double value) {
            validate("addDouble("+value+")");
          }

          @Override
          public void addFloat(float value) {
            validate("addFloat("+value+")");
          }

          @Override
          public void addInt(int value) {
            validate("addInt("+value+")");
          }

          @Override
          public void addLong(long value) {
            validate("addLong("+value+")");
          }
        };
      }

      @Override
      public Converter convertGroupType(final List<GroupType> path, final GroupType groupType, final List<Converter> children) {
        return new GroupConverter() {

          private void validate(String message) {
            ExpectationValidatingConverter.this.validate(path(path, groupType) + message);
          }

          @Override
          public void start() {
            validate("start()");
          }

          @Override
          public void end() {
            validate("end()");
          }

          @Override
          public Converter getConverter(int fieldIndex) {
            return children.get(fieldIndex);
          }

        };
      }

      @Override
      public Converter convertMessageType(MessageType messageType, final List<Converter> children) {
        return new GroupConverter() {

          @Override
          public Converter getConverter(int fieldIndex) {
            return children.get(fieldIndex);
          }

          @Override
          public void start() {
            validate("startMessage()");
          }

          @Override
          public void end() {
            validate("endMessage()");
          }
        };
      }
    });
  }

  @Override
  public Void getCurrentRecord() {
    return null;
  }

  private String path(List<GroupType> path, Type type) {
    String pathString = "";
    if (path.size() > 0) {
      for (int i = 1; i < path.size(); i++) {
        pathString += path.get(i).getName() + ".";
      }
    }
    pathString += type.getName() + ".";
    return pathString;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }

}
