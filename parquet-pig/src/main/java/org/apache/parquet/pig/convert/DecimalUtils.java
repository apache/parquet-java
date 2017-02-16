package org.apache.parquet.pig.convert;

import java.nio.ByteBuffer;
import java.math.BigInteger;
import java.math.BigDecimal;
import static java.lang.Math.pow;

import org.apache.parquet.io.api.Binary;

/*
 * Conversion between Parquet Decimal Type to Java BigDecimal in Pig
 * Code Based on the Apache Spark ParquetRowConverter.java
 * 
 *
 */

public class DecimalUtils {

  public static BigDecimal binaryToDecimal(Binary value, int precision, int scale) {
    if (precision <= 18) {
      ByteBuffer buffer = value.toByteBuffer();
      byte[] bytes = buffer.array();
      int start = buffer.arrayOffset() + buffer.position();
      int end = buffer.arrayOffset() + buffer.limit();
      long unscaled = 0L;
      int i = start;
      while ( i < end ) {
        unscaled = ( unscaled << 8 | bytes[i] & 0xff );
        i++;
      }
      int bits = 8*(end - start);
      long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
      if (unscaledNew <= -pow(10,18) || unscaledNew >= pow(10,18)) {
        return new BigDecimal(unscaledNew);
      } else {
        return BigDecimal.valueOf(unscaledNew / pow(10,scale));
      }
    } else {
      return new BigDecimal(new BigInteger(value.getBytes()), scale);
    }
  }
}
