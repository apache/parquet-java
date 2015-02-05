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
package parquet.column;

import parquet.ParquetRuntimeException;

/**
 * Thrown if the specified column is unknown in the underlying storage
 *
 * @author Julien Le Dem
 */
public class UnknownColumnException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  private final ColumnDescriptor descriptor;

  public UnknownColumnException(ColumnDescriptor descriptor) {
    super("Column not found: " + descriptor.toString());
    this.descriptor = descriptor;
  }

  public ColumnDescriptor getDescriptor() {
    return descriptor;
  }

}
