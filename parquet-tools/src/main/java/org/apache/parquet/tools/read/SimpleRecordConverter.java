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
package org.apache.parquet.tools.read;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import static java.util.Optional.of;

public class SimpleRecordConverter extends GroupConverter {
  private final Converter converters[];
  private final String name;
  private final SimpleRecordConverter parent;
  protected SimpleRecord record;

  public SimpleRecordConverter(GroupType schema) {
    this(schema, null, null);
  }

  public SimpleRecordConverter(GroupType schema, String name, SimpleRecordConverter parent) {
    this.converters = new Converter[schema.getFieldCount()];
    this.parent = parent;
    this.name = name;

    int i = 0;
    for (Type field: schema.getFields()) {
      converters[i++] = createConverter(field);
    }
  }

  private Converter createConverter(Type field) {
    LogicalTypeAnnotation ltype = field.getLogicalTypeAnnotation();

    if (field.isPrimitive()) {
      if (ltype != null) {
        return ltype.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Converter>() {
          @Override
          public Optional<Converter> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
            return of(new StringConverter(field.getName()));
          }

          @Override
          public Optional<Converter> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            int scale = decimalLogicalType.getScale();
            return of(new DecimalConverter(field.getName(), scale));
          }
        }).orElse(new SimplePrimitiveConverter(field.getName()));
      }
      return new SimplePrimitiveConverter(field.getName());
    }

    GroupType groupType = field.asGroupType();
    if (ltype != null) {
      return ltype.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Converter>() {
        @Override
        public Optional<Converter> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
          return of(new SimpleMapRecordConverter(groupType, field.getName(), SimpleRecordConverter.this));
        }

        @Override
        public Optional<Converter> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
          return of(new SimpleListRecordConverter(groupType, field.getName(), SimpleRecordConverter.this));
        }
      }).orElse(new SimpleRecordConverter(groupType, field.getName(), this));
    }
    return new SimpleRecordConverter(groupType, field.getName(), this);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  SimpleRecord getCurrentRecord() {
    return record;
  }

  @Override
  public void start() {
    record = new SimpleRecord();
  }

  @Override
  public void end() {
    if (parent != null) {
      parent.getCurrentRecord().add(name, record);
    }
  }

  private class SimplePrimitiveConverter extends PrimitiveConverter {
    protected final String name;

    public SimplePrimitiveConverter(String name) {
      this.name = name;
    }

    @Override
    public void addBinary(Binary value) {
      record.add(name, value.getBytes());
    }

    @Override
    public void addBoolean(boolean value) {
      record.add(name, value);
    }

    @Override
    public void addDouble(double value) {
      record.add(name, value);
    }

    @Override
    public void addFloat(float value) {
      record.add(name, value);
    }

    @Override
    public void addInt(int value) {
      record.add(name, value);
    }

    @Override
    public void addLong(long value) {
      record.add(name, value);
    }
  }

  private class StringConverter extends SimplePrimitiveConverter {
    public StringConverter(String name) {
      super(name);
    }

    @Override
    public void addBinary(Binary value) {
      record.add(name, value.toStringUsingUTF8());
    }
  }

  private class DecimalConverter extends SimplePrimitiveConverter {
    private final int scale;

    public DecimalConverter(String name, int scale) {
      super(name);
      this.scale = scale;
    }

    @Override
    public void addBinary(Binary value) {
      record.add(name, new BigDecimal(new BigInteger(value.getBytes()), scale));
    }

    @Override
    public void addInt(int value) {
      record.add(name, BigDecimal.valueOf(value).movePointLeft(scale));
    }

    @Override
    public void addLong(long value) {
      record.add(name, BigDecimal.valueOf(value).movePointLeft(scale));
    }
  }
}

