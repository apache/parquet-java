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
package org.apache.parquet.schema;

import java.util.List;
import java.util.Map;

/**
 * This class decorates the class 'Type' by adding a Map field 'metadata'.
 *
 * This decoration is needed to add metadata to each column without changing existing class 'MessageType', which is used
 * extensively. Here is the example usage to add column metadata to schema with type of 'MessageType'.
 *
 * MessageType oldSchema = ...
 * Map metadata = ...
 * List newFields = new ArrayList();
 * for (Type field = oldSchema.getFields()) {
 *     Type newField = new ExtType(field);
 *     newField.setMetadata(metadata);
 *     newFields.add(newField);
 * }
 * MessageType newSchema = new MessageType(oldSchema.getName(), newFields);
 *
 * The implementation is mostly following decoration pattern. Most of the methods are just thin wrappers of existing
 * implementation of PrimitiveType or GroupType.
 */
public class ExtType<T> extends Type {
  private Type type;
  private Map<String, T> metadata;

  public ExtType(Type type) {
    super(type.getName(), type.getRepetition(), type.getOriginalType(), type.getId());
    this.type = type;
  }

  public ExtType(Type type, String name) {
    super(name, type.getRepetition(), OriginalType.UINT_64, type.getId());
    this.type = new PrimitiveType(type.getRepetition(), type.asPrimitiveType().getPrimitiveTypeName(), name);
  }

  public Type withId(int id) {
    return this.type.withId(id);
  }

  public boolean isPrimitive() {
    return this.type.isPrimitive();
  }

  public void writeToStringBuilder(StringBuilder sb, String indent) {
    this.type.writeToStringBuilder(sb, indent);
  }

  public void accept(TypeVisitor visitor) {
    this.type.accept(visitor);
  }

  /** @deprecated */
  @Deprecated
  protected int typeHashCode() {
    return this.type.hashCode();
  }

  /** @deprecated */
  @Deprecated
  protected boolean typeEquals(Type other) {
    return this.type.typeEquals(other);
  }

  protected boolean equals(Type other) {
    return this.type.equals(other);
  }

  public int getMaxRepetitionLevel(String[] path, int i) {
    return this.type.getMaxRepetitionLevel(path, i);
  }

  public int getMaxDefinitionLevel(String[] path, int i) {
    return this.type.getMaxDefinitionLevel(path, i);
  }

  public Type getType(String[] path, int i) {
    return this.type.getType(path, i);
  }

  protected List<String[]> getPaths(int depth) {
    return this.type.getPaths(depth);
  }

  void checkContains(Type subType) {
    this.type.checkContains(subType);
  }

  public <T> T convert(List<GroupType> path, TypeConverter<T> converter) {
    return this.type.convert(path, converter);
  }

  protected boolean containsPath(String[] path, int depth) {
    return this.type.containsPath(path, depth);
  }

  protected Type union(Type toMerge) {
    return this.type.union(toMerge);
  }

  protected Type union(Type toMerge, boolean strict) {
    return this.type.union(toMerge, strict);
  }

  public PrimitiveType asPrimitiveType() {
    if (!this.type.isPrimitive()) {
      throw new ClassCastException(this + " is not primitive");
    } else {
      return (PrimitiveType)this.type;
    }
  }

  public GroupType asGroupType() {
    if (this.type.isPrimitive()) {
      throw new ClassCastException(this + " is not a group");
    } else {
      return (GroupType)this.type;
    }
  }

  public void setMetadata(Map<String, T> metadata) {
    this.metadata = metadata;
  }

  public Map<String, T> getMetadata() {
    return this.metadata;
  }
}
