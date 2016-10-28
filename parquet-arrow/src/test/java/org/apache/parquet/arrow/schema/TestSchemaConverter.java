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
package org.apache.parquet.arrow.schema;

import static java.util.Arrays.asList;
import static org.apache.parquet.schema.OriginalType.DATE;
import static org.apache.parquet.schema.OriginalType.DECIMAL;
import static org.apache.parquet.schema.OriginalType.INTERVAL;
import static org.apache.parquet.schema.OriginalType.INT_16;
import static org.apache.parquet.schema.OriginalType.INT_32;
import static org.apache.parquet.schema.OriginalType.INT_64;
import static org.apache.parquet.schema.OriginalType.INT_8;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.OriginalType.TIME_MILLIS;
import static org.apache.parquet.schema.OriginalType.UINT_16;
import static org.apache.parquet.schema.OriginalType.UINT_32;
import static org.apache.parquet.schema.OriginalType.UINT_64;
import static org.apache.parquet.schema.OriginalType.UINT_8;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.flatbuf.IntervalUnit;
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.flatbuf.TimeUnit;
import org.apache.arrow.flatbuf.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.arrow.schema.SchemaMapping.ListTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.PrimitiveTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.RepeatedTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.StructTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.TypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.TypeMappingVisitor;
import org.apache.parquet.arrow.schema.SchemaMapping.UnionTypeMapping;
import org.apache.parquet.example.Paper;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import junit.framework.Assert;

/**
 * @see SchemaConverter
 */
public class TestSchemaConverter {

  private static Field field(String name, boolean nullable, ArrowType type, Field... children) {
    return new Field(name, nullable, type, asList(children));
  }

  private static Field field(String name, ArrowType type, Field... children) {
    return field(name, true, type, children);
  }

  private final Schema complexArrowSchema = new Schema(asList(
      field("a", false, new ArrowType.Int(8, true)),
      field("b", new ArrowType.Struct_(),
          field("c", new ArrowType.Int(16, true)),
          field("d", new ArrowType.Utf8())),
      field("e", new ArrowType.List(), field(null, new ArrowType.Date())),
      field("f", new ArrowType.FloatingPoint(Precision.SINGLE)),
      field("g", new ArrowType.Timestamp(TimeUnit.MILLISECOND)),
      field("h", new ArrowType.Interval(IntervalUnit.DAY_TIME))
      ));
  private final MessageType complexParquetSchema = Types.buildMessage()
      .addField(Types.optional(INT32).as(INT_8).named("a"))
      .addField(Types.optionalGroup()
          .addField(Types.optional(INT32).as(INT_16).named("c"))
          .addField(Types.optional(BINARY).as(UTF8).named("d"))
          .named("b"))
      .addField(Types.optionalList().
          setElementType(Types.optional(INT32).as(DATE).named("element"))
          .named("e"))
      .addField(Types.optional(FLOAT).named("f"))
      .addField(Types.optional(INT64).as(TIMESTAMP_MILLIS).named("g"))
      .addField(Types.optional(FIXED_LEN_BYTE_ARRAY).length(12).as(INTERVAL).named("h"))
      .named("root");

  private final Schema allTypesArrowSchema = new Schema(asList(
      field("a", false, new ArrowType.Null()),
      field("b", new ArrowType.Struct_(), field("ba", new ArrowType.Null())),
      field("c", new ArrowType.List(), field("ca", new ArrowType.Null())),
      field("d", new ArrowType.Union(UnionMode.Sparse, new int[] {1, 2, 3}), field("da", new ArrowType.Null())),
      field("e", new ArrowType.Int(8, true)),
      field("e1", new ArrowType.Int(16, true)),
      field("e2", new ArrowType.Int(32, true)),
      field("e3", new ArrowType.Int(64, true)),
      field("e4", new ArrowType.Int(8, false)),
      field("e5", new ArrowType.Int(16, false)),
      field("e6", new ArrowType.Int(32, false)),
      field("e7", new ArrowType.Int(64, false)),
      field("f", new ArrowType.FloatingPoint(Precision.SINGLE)),
      field("f1", new ArrowType.FloatingPoint(Precision.DOUBLE)),
      field("g", new ArrowType.Utf8()),
      field("h", new ArrowType.Binary()),
      field("i", new ArrowType.Bool()),
      field("j", new ArrowType.Decimal(5, 5)),
      field("j1", new ArrowType.Decimal(15, 5)),
      field("j2", new ArrowType.Decimal(25, 5)),
      field("k", new ArrowType.Date()),
      field("l", new ArrowType.Time()),
      field("m", new ArrowType.Timestamp(TimeUnit.MILLISECOND)),
      field("n", new ArrowType.Interval(IntervalUnit.DAY_TIME)),
      field("n1", new ArrowType.Interval(IntervalUnit.YEAR_MONTH))
      ));
  private final MessageType allTypesParquetSchema = Types.buildMessage()
      .addField(Types.optional(BINARY).named("a"))
      .addField(Types.optionalGroup()
          .addField(Types.optional(BINARY).named("ba"))
          .named("b"))
      .addField(Types.optionalList().
          setElementType(Types.optional(BINARY).named("element"))
          .named("c"))
      .addField(Types.optionalGroup()
          .addField(Types.optional(BINARY).named("da"))
          .named("d"))
      .addField(Types.optional(INT32).as(INT_8).named("e"))
      .addField(Types.optional(INT32).as(INT_16).named("e1"))
      .addField(Types.optional(INT32).as(INT_32).named("e2"))
      .addField(Types.optional(INT64).as(INT_64).named("e3"))
      .addField(Types.optional(INT32).as(UINT_8).named("e4"))
      .addField(Types.optional(INT32).as(UINT_16).named("e5"))
      .addField(Types.optional(INT32).as(UINT_32).named("e6"))
      .addField(Types.optional(INT64).as(UINT_64).named("e7"))
      .addField(Types.optional(FLOAT).named("f"))
      .addField(Types.optional(DOUBLE).named("f1"))
      .addField(Types.optional(BINARY).as(UTF8).named("g"))
      .addField(Types.optional(BINARY).named("h"))
      .addField(Types.optional(BOOLEAN).named("i"))
      .addField(Types.optional(INT32).as(DECIMAL).precision(5).scale(5).named("j"))
      .addField(Types.optional(INT64).as(DECIMAL).precision(15).scale(5).named("j1"))
      .addField(Types.optional(BINARY).as(DECIMAL).precision(25).scale(5).named("j2"))
      .addField(Types.optional(INT32).as(DATE).named("k"))
      .addField(Types.optional(INT32).as(TIME_MILLIS).named("l"))
      .addField(Types.optional(INT64).as(TIMESTAMP_MILLIS).named("m"))
      .addField(Types.optional(FIXED_LEN_BYTE_ARRAY).length(12).as(INTERVAL).named("n"))
      .addField(Types.optional(FIXED_LEN_BYTE_ARRAY).length(12).as(INTERVAL).named("n1"))
      .named("root");

  private final Schema supportedTypesArrowSchema = new Schema(asList(
      field("b", new ArrowType.Struct_(), field("ba", new ArrowType.Binary())),
      field("c", new ArrowType.List(), field(null, new ArrowType.Binary())),
      field("e", new ArrowType.Int(8, true)),
      field("e1", new ArrowType.Int(16, true)),
      field("e2", new ArrowType.Int(32, true)),
      field("e3", new ArrowType.Int(64, true)),
      field("e4", new ArrowType.Int(8, false)),
      field("e5", new ArrowType.Int(16, false)),
      field("e6", new ArrowType.Int(32, false)),
      field("e7", new ArrowType.Int(64, false)),
      field("f", new ArrowType.FloatingPoint(Precision.SINGLE)),
      field("f1", new ArrowType.FloatingPoint(Precision.DOUBLE)),
      field("g", new ArrowType.Utf8()),
      field("h", new ArrowType.Binary()),
      field("i", new ArrowType.Bool()),
      field("j", new ArrowType.Decimal(5, 5)),
      field("j1", new ArrowType.Decimal(15, 5)),
      field("j2", new ArrowType.Decimal(25, 5)),
      field("k", new ArrowType.Date()),
      field("l", new ArrowType.Time()),
      field("m", new ArrowType.Timestamp(TimeUnit.MILLISECOND))
      ));

  private final MessageType supportedTypesParquetSchema = Types.buildMessage()
      .addField(Types.optionalGroup()
          .addField(Types.optional(BINARY).named("ba"))
          .named("b"))
      .addField(Types.optionalList().
          setElementType(Types.optional(BINARY).named("element"))
          .named("c"))
      .addField(Types.optional(INT32).as(INT_8).named("e"))
      .addField(Types.optional(INT32).as(INT_16).named("e1"))
      .addField(Types.optional(INT32).as(INT_32).named("e2"))
      .addField(Types.optional(INT64).as(INT_64).named("e3"))
      .addField(Types.optional(INT32).as(UINT_8).named("e4"))
      .addField(Types.optional(INT32).as(UINT_16).named("e5"))
      .addField(Types.optional(INT32).as(UINT_32).named("e6"))
      .addField(Types.optional(INT64).as(UINT_64).named("e7"))
      .addField(Types.optional(FLOAT).named("f"))
      .addField(Types.optional(DOUBLE).named("f1"))
      .addField(Types.optional(BINARY).as(UTF8).named("g"))
      .addField(Types.optional(BINARY).named("h"))
      .addField(Types.optional(BOOLEAN).named("i"))
      .addField(Types.optional(INT32).as(DECIMAL).precision(5).scale(5).named("j"))
      .addField(Types.optional(INT64).as(DECIMAL).precision(15).scale(5).named("j1"))
      .addField(Types.optional(BINARY).as(DECIMAL).precision(25).scale(5).named("j2"))
      .addField(Types.optional(INT32).as(DATE).named("k"))
      .addField(Types.optional(INT32).as(TIME_MILLIS).named("l"))
      .addField(Types.optional(INT64).as(TIMESTAMP_MILLIS).named("m"))
      .named("root");

  private final Schema paperArrowSchema = new Schema(asList(
      field("DocId", false, new ArrowType.Int(64, true)),
      field("Links", new ArrowType.Struct_(),
          field("Backward", false, new ArrowType.List(), field(null, false, new ArrowType.Int(64, true))),
          field("Forward", false, new ArrowType.List(), field(null, false, new ArrowType.Int(64, true)))
      ),
      field("Name", false, new ArrowType.List(),
          field(null, false, new ArrowType.Struct_(),
              field("Language", false, new ArrowType.List(),
                  field(null, false, new ArrowType.Struct_(),
                      field("Code", false, new ArrowType.Binary()),
                      field("Country", new ArrowType.Binary())
                  )
               ),
               field("Url", new ArrowType.Binary())
          )
      )
  ));

  private SchemaConverter converter = new SchemaConverter();

  @Test
  public void testComplexArrowToParquet() throws IOException {
    MessageType parquet = converter.fromArrow(complexArrowSchema).getParquetSchema();
    Assert.assertEquals(complexParquetSchema.toString(), parquet.toString()); // easier to read
    Assert.assertEquals(complexParquetSchema, parquet);
  }

  @Test
  public void testAllArrowToParquet() throws IOException {
    MessageType parquet = converter.fromArrow(allTypesArrowSchema).getParquetSchema();
    Assert.assertEquals(allTypesParquetSchema.toString(), parquet.toString()); // easier to read
    Assert.assertEquals(allTypesParquetSchema, parquet);
  }

  @Test
  public void testSupportedParquetToArrow() throws IOException {
    Schema arrow = converter.fromParquet(supportedTypesParquetSchema).getArrowSchema();
    assertEquals(supportedTypesArrowSchema, arrow);
  }

  @Test
  public void testRepeatedParquetToArrow() throws IOException {
    Schema arrow = converter.fromParquet(Paper.schema).getArrowSchema();
    assertEquals(paperArrowSchema, arrow);
  }

  public void assertEquals(Schema left, Schema right) {
    compareFields(left.getFields(), right.getFields());
    Assert.assertEquals(left, right);
  }

  /**
   * for more pinpointed error on what is different
   * @param left
   * @param right
   */
  private void compareFields(List<Field> left, List<Field> right) {
    Assert.assertEquals(left + "\n" + right, left.size(), right.size());
    int size = left.size();
    for (int i = 0; i < size; i++) {
      Field expectedField = left.get(i);
      Field field = right.get(i);
      compareFields(expectedField.getChildren(), field.getChildren());
      Assert.assertEquals(expectedField, field);
    }
  }

  @Test
  public void testAllMap() throws IOException {
    SchemaMapping map = converter.map(allTypesArrowSchema, allTypesParquetSchema);
    Assert.assertEquals("p, s<p>, l<p>, u<p>, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p", toSummaryString(map));
  }

  private String toSummaryString(SchemaMapping map) {
    List<TypeMapping> fields = map.getChildren();
    return toSummaryString(fields);
  }

  private String toSummaryString(List<TypeMapping> fields) {
    final StringBuilder sb = new StringBuilder();
    for (TypeMapping typeMapping : fields) {
      if (sb.length() != 0) {
        sb.append(", ");
      }
      sb.append(
          typeMapping.accept(new TypeMappingVisitor<String>() {
            @Override
            public String visit(PrimitiveTypeMapping primitiveTypeMapping) {
              return "p";
            }

            @Override
            public String visit(StructTypeMapping structTypeMapping) {
              return "s";
            }

            @Override
            public String visit(UnionTypeMapping unionTypeMapping) {
              return "u";
            }

            @Override
            public String visit(ListTypeMapping listTypeMapping) {
              return "l";
            }

            @Override
            public String visit(RepeatedTypeMapping repeatedTypeMapping) {
              return "r";
            }
          })
      );
      if (typeMapping.getChildren() != null && !typeMapping.getChildren().isEmpty()) {
        sb.append("<").append(toSummaryString(typeMapping.getChildren())).append(">");
      }
    }
    return sb.toString();
  }

  @Test
  public void testRepeatedMap() throws IOException {
    SchemaMapping map = converter.map(paperArrowSchema, Paper.schema);
    Assert.assertEquals("p, s<r<p>, r<p>>, r<s<r<s<p, p>>, p>>", toSummaryString(map));
  }
}
