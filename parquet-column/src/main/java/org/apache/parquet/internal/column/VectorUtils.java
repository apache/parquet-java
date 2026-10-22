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
package org.apache.parquet.internal.column;

import java.util.Arrays;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ParquetDecodingException;

public final class VectorUtils {
  private VectorUtils() {}

  public static void checkCounts(ColumnDescriptor path, int numElements, long rows, long values, long nulls) {
    long declaredNulls = nulls;
    boolean invalid = rows < 0 || values < 0 || nulls < -1 || nulls > values;
    if (!invalid && path.getMaxRepetitionLevel() == 1) {
      if (path.getMaxDefinitionLevel() == 1) {
        invalid = nulls > 0;
        nulls = 0;
      }
      if (nulls >= 0) {
        long present = values - nulls;
        invalid |= nulls > rows || present % numElements != 0 || present / numElements != rows - nulls;
      } else {
        long extra = values - rows;
        invalid |= values < rows
            || (numElements == 1
                ? values != rows
                : extra % (numElements - 1) != 0 || extra / (numElements - 1) > rows);
      }
    } else if (!invalid && nulls >= 0) {
      invalid = (values - nulls) % numElements != 0;
    }
    if (invalid) {
      throw new ParquetDecodingException("Invalid VECTOR counts for " + Arrays.toString(path.getPath())
          + ": rows=" + rows + ", values=" + values + ", nulls=" + declaredNulls
          + ", length=" + numElements);
    }
  }
}
