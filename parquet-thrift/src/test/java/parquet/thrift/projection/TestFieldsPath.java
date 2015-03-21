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
package parquet.thrift.projection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.elephantbird.thrift.test.TestStructInMap;

import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftType.BoolType;
import parquet.thrift.struct.ThriftType.ByteType;
import parquet.thrift.struct.ThriftType.DoubleType;
import parquet.thrift.struct.ThriftType.EnumType;
import parquet.thrift.struct.ThriftType.I16Type;
import parquet.thrift.struct.ThriftType.I32Type;
import parquet.thrift.struct.ThriftType.I64Type;
import parquet.thrift.struct.ThriftType.ListType;
import parquet.thrift.struct.ThriftType.MapType;
import parquet.thrift.struct.ThriftType.SetType;
import parquet.thrift.struct.ThriftType.StringType;
import parquet.thrift.struct.ThriftType.StructType;

import static org.junit.Assert.assertEquals;

public class TestFieldsPath {
  @Test
  public void testFieldsPath() {
    StructType person = ThriftSchemaConverter.toStructType(Person.class);

    List<String> paths = PrimitivePathVisitor.visit(person, ".");
    assertEquals(Arrays.asList("name.first_name", "name.last_name", "id", "email", "phones.number", "phones.type"),
        paths);

    paths = PrimitivePathVisitor.visit(person, "/");
    assertEquals(Arrays.asList("name/first_name", "name/last_name", "id", "email", "phones/number", "phones/type"),
        paths);

    StructType structInMap = ThriftSchemaConverter.toStructType(TestStructInMap.class);

    paths = PrimitivePathVisitor.visit(structInMap, ".");
    assertEquals(Arrays.asList("name", "names.key", "names.value.name.first_name", "names.value.name.last_name",
            "names.value.phones.key", "names.value.phones.value", "name_to_id.key", "name_to_id.value"), paths);

    paths = PrimitivePathVisitor.visit(structInMap, "/");
    assertEquals(Arrays.asList("name", "names/key", "names/value/name/first_name", "names/value/name/last_name",
        "names/value/phones/key", "names/value/phones/value", "name_to_id/key", "name_to_id/value"), paths);

  }

  private static class PrimitivePathVisitor implements ThriftType.TypeVisitor {
    private List<String> paths = new ArrayList<String>();
    private FieldsPath path = new FieldsPath();
    private String delim;

    private PrimitivePathVisitor(String delim) {
      this.delim = delim;
    }

    public static List<String> visit(StructType s, String delim) {
      PrimitivePathVisitor v = new PrimitivePathVisitor(delim);
      s.accept(v);
      return v.getPaths();
    }

    public List<String> getPaths() {
      return paths;
    }

    @Override
    public void visit(MapType mapType) {
      ThriftField key = mapType.getKey();
      ThriftField value = mapType.getValue();
      path.push(key);
      key.getType().accept(this);
      path.pop();
      path.push(value);
      value.getType().accept(this);
      path.pop();
    }

    @Override
    public void visit(SetType setType) {
      setType.getValues().getType().accept(this);
    }

    @Override
    public void visit(ListType listType) {
      listType.getValues().getType().accept(this);
    }

    @Override
    public void visit(StructType structType) {
      for (ThriftField child : structType.getChildren()) {
        path.push(child);
        child.getType().accept(this);
        path.pop();
      }
    }

    private void visitPrimitive() {
      paths.add(path.toDelimitedString(delim));
    }

    @Override
    public void visit(EnumType enumType) {
      visitPrimitive();
    }

    @Override
    public void visit(BoolType boolType) {
      visitPrimitive();
    }

    @Override
    public void visit(ByteType byteType) {
      visitPrimitive();
    }

    @Override
    public void visit(DoubleType doubleType) {
      visitPrimitive();
    }

    @Override
    public void visit(I16Type i16Type) {
      visitPrimitive();
    }

    @Override
    public void visit(I32Type i32Type) {
      visitPrimitive();
    }

    @Override
    public void visit(I64Type i64Type) {
      visitPrimitive();
    }

    @Override
    public void visit(StringType stringType) {
      visitPrimitive();
    }
  }
}
