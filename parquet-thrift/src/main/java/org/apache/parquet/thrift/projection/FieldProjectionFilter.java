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
package org.apache.parquet.thrift.projection;


/**
 * A field projection filter decides whether a thrift field (column) should
 * be included when reading thrift data. It is used to implement projection push down.
 *
 * See {@link StrictFieldProjectionFilter}
 */
public interface FieldProjectionFilter {

  /**
   * Decide whether to keep the field (column) represented by path.
   * This path always represents a primitive (leaf node) path.
   *
   * @param path the path to the field (column)
   * @return true to keep, false to discard (project out)
   */
  boolean keep(FieldsPath path);

  /**
   * Should throw a ThriftProjectionException if this FieldProjectionFilter has remaining patterns / columns
   * that didn't match any of paths passed to {@link #keep(FieldsPath)}.
   *
   * Will be called once after all paths have been passed to {@link #keep(FieldsPath)}.
   */
  void assertNoUnmatchedPatterns() throws ThriftProjectionException;

  /**
   * A filter that keeps all of the columns.
   */
  public static final FieldProjectionFilter ALL_COLUMNS = new FieldProjectionFilter() {
    @Override
    public boolean keep(FieldsPath path) {
      return true;
    }

    @Override
    public void assertNoUnmatchedPatterns() throws ThriftProjectionException {

    }
  };
}
