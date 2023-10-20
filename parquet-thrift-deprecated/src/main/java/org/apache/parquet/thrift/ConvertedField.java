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
package org.apache.parquet.thrift;

import java.util.Objects;

import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.schema.Type;
import org.apache.parquet.thrift.projection.FieldsPath;

/**
 * This is the return value for the recursion done in {@link ThriftSchemaConvertVisitor}
 * It represents a field that has been converted from a {@link org.apache.parquet.thrift.struct.ThriftType}
 * to a {@link org.apache.parquet.schema.MessageType}, as well as whether this field is being
 * projected away, kept, or kept only because we cannot safely drop all of its fields.
 *
 * This interface is essentially a union of {keep, drop, sentinelUnion}
 */
@Deprecated
public interface ConvertedField {

  /**
   * The path from the root of the schema to this field.
   *
   * @return the fields path
   */
  FieldsPath path();

  boolean isKeep();
  Keep asKeep();

  boolean isDrop();
  Drop asDrop();

  boolean isSentinelUnion();
  SentinelUnion asSentinelUnion();

  static abstract class ConvertedFieldBase implements ConvertedField {
    private final FieldsPath path;

    protected ConvertedFieldBase(FieldsPath path) {
      this.path = Objects.requireNonNull(path, "path cannot be null");
    }

    @Override
    public boolean isKeep() {
      return false;
    }

    @Override
    public Keep asKeep() {
      throw new ShouldNeverHappenException("asKeep called on " + this);
    }

    @Override
    public boolean isDrop() {
      return false;
    }

    @Override
    public Drop asDrop() {
      throw new ShouldNeverHappenException("asDrop called on " + this);
    }

    @Override
    public boolean isSentinelUnion() {
      return false;
    }

    @Override
    public SentinelUnion asSentinelUnion() {
      throw new ShouldNeverHappenException("asSentinelUnion called on " + this);
    }

    @Override
    public FieldsPath path() {
      return path;
    }
  }

  /**
   * Signals that the user explicitly requested either this field or one of its children.
   */
  public static final class Keep extends ConvertedFieldBase {
    private final Type type;

    public Keep(FieldsPath path, Type type) {
      super(path);
      this.type = Objects.requireNonNull(type, "type cannot be null");
    }

    @Override
    public boolean isKeep() {
      return true;
    }

    @Override
    public Keep asKeep() {
      return this;
    }

    public Type getType() {
      return type;
    }
  }

  /**
   * Signals that the user did not explicitly request this field nor its children, but
   * we carry the converted type info anyway in case it is not possible to drop this union
   * entirely.
   */
  public static final class SentinelUnion extends ConvertedFieldBase {
    private final Type type;

    public SentinelUnion(FieldsPath path, Type type) {
      super(path);
      this.type = Objects.requireNonNull(type, "type cannot be null");
    }

    @Override
    public boolean isSentinelUnion() {
      return true;
    }

    @Override
    public SentinelUnion asSentinelUnion() {
      return this;
    }

    public Type getType() {
      return type;
    }
  }

  /**
   * Signals the user did not request this field nor its children.
   */
  public static final class Drop extends ConvertedFieldBase {
    public Drop(FieldsPath path) {
      super(path);
    }

    @Override
    public boolean isDrop() {
      return true;
    }

    @Override
    public Drop asDrop() {
      return this;
    }
  }
}

