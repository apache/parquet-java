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

import org.apache.parquet.schema.Type;
import org.apache.parquet.thrift.projection.FieldsPath;

import static org.apache.parquet.Preconditions.checkNotNull;

public class ConvertedField {
  private final Type type;
  private final FieldsPath path;
  private final boolean keep;
  private final boolean isSentinel;

  private ConvertedField(Type type, FieldsPath path, boolean keep, boolean isSentinel) {
    this.type = type;
    this.path = path;
    this.keep = keep;
    this.isSentinel = isSentinel;
  }

  public static ConvertedField keep(Type type, FieldsPath path) {
    return new ConvertedField(checkNotNull(type, "type"), checkNotNull(path, "path"), true, false);
  }

  public static ConvertedField sentinelKeep(Type type, FieldsPath path) {
    return new ConvertedField(checkNotNull(type, "type"), checkNotNull(path, "path"), false, true);
  }

  public static ConvertedField drop(FieldsPath path) {
    return new ConvertedField(null, checkNotNull(path, "path"), false, false);
  }

  public Type getType() {
    if (type == null) {
      throw new IllegalArgumentException("getType called on a projected field");
    }
    return type;
  }

  public FieldsPath getPath() {
    return path;
  }

  public boolean keep() {
    return keep;
  }

  public boolean isSentinel() {
    return isSentinel;
  }
}
