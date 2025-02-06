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

package org.apache.parquet.pig.convert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.parquet.io.api.Binary;

/*
 * Conversion between Parquet Decimal Type to Java BigDecimal in Pig
 * Code Based on the Apache Spark ParquetRowConverter.scala
 *
 *
 */

public class DecimalUtils {

  public static BigDecimal binaryToDecimal(Binary value, int precision, int scale) {
    /*
     * Precision <= 18 checks for the max number of digits for an unscaled long,
     * else treat with big integer conversion
     */
    if (precision <= 18) {
      ByteBuffer buffer = value.toByteBuffer();
      byte[] bytes = buffer.array();
      int start = buffer.arrayOffset() + buffer.position();
      int end = buffer.arrayOffset() + buffer.limit();
      long unscaled = 0L;
      int i = start;
      while (i < end) {
        unscaled = (unscaled << 8 | bytes[i] & 0xff);
        i++;
      }
      int bits = 8 * (end - start);
      long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
      return BigDecimal.valueOf(unscaledNew, scale);
    } else {
      return new BigDecimal(new BigInteger(value.getBytes()), scale);
    }
  }
}
