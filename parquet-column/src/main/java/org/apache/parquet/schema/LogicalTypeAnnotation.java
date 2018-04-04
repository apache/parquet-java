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

import org.apache.parquet.format.BsonType;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DateType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.EnumType;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.JsonType;
import org.apache.parquet.format.ListType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MapType;
import org.apache.parquet.format.MicroSeconds;
import org.apache.parquet.format.MilliSeconds;
import org.apache.parquet.format.NullType;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.TimeType;
import org.apache.parquet.format.TimestampType;

import java.util.Objects;

public interface LogicalTypeAnnotation {
  /**
   * Convert this parquet-mr logical type to parquet-format LogicalType.
   *
   * @return the parquet-format LogicalType representation of this logical type implementation
   */
  LogicalType toLogicalType();

  /**
   * Convert this parquet-mr logical type to parquet-format ConvertedType.
   *
   * @return the parquet-format ConvertedType representation of this logical type implementation
   */
  ConvertedType toConvertedType();

  /**
   * Convert this logical type to old logical type representation in parquet-mr (if there's any).
   * Those logical type implementations, which don't have a corresponding mapping should return null.
   *
   * @return the OriginalType representation of the new logical type, or null if there's none
   */
  OriginalType toOriginalType();

  /**
   * Helper method to convert the old representation of logical types (OriginalType) to new logical type.
   */
  static LogicalTypeAnnotation fromOriginalType(OriginalType originalType, DecimalMetadata decimalMetadata) {
    if (originalType == null) {
      return null;
    }
    switch (originalType) {
      case UTF8:
        return StringLogicalTypeAnnotation.create();
      case MAP:
        return MapLogicalTypeAnnotation.create();
      case DECIMAL:
        int scale = (decimalMetadata == null ? 0 : decimalMetadata.getScale());
        int precision = (decimalMetadata == null ? 0 : decimalMetadata.getPrecision());
        return DecimalLogicalTypeAnnotation.create(scale, precision);
      case LIST:
        return ListLogicalTypeAnnotation.create();
      case DATE:
        return DateLogicalTypeAnnotation.create();
      case INTERVAL:
        return IntervalLogicalTypeAnnotation.create();
      case TIMESTAMP_MILLIS:
        return TimestampLogicalTypeAnnotation.create(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
      case TIMESTAMP_MICROS:
        return TimestampLogicalTypeAnnotation.create(true, LogicalTypeAnnotation.TimeUnit.MICROS);
      case TIME_MILLIS:
        return TimeLogicalTypeAnnotation.create(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
      case TIME_MICROS:
        return TimeLogicalTypeAnnotation.create(true, LogicalTypeAnnotation.TimeUnit.MICROS);
      case UINT_8:
        return IntLogicalTypeAnnotation.create((byte) 8, false);
      case UINT_16:
        return IntLogicalTypeAnnotation.create((byte) 16, false);
      case UINT_32:
        return IntLogicalTypeAnnotation.create((byte) 32, false);
      case UINT_64:
        return IntLogicalTypeAnnotation.create((byte) 64, false);
      case INT_8:
        return IntLogicalTypeAnnotation.create((byte) 8, true);
      case INT_16:
        return IntLogicalTypeAnnotation.create((byte) 16, true);
      case INT_32:
        return IntLogicalTypeAnnotation.create((byte) 32, true);
      case INT_64:
        return IntLogicalTypeAnnotation.create((byte) 64, true);
      case ENUM:
        return EnumLogicalTypeAnnotation.create();
      case JSON:
        return JsonLogicalTypeAnnotation.create();
      case BSON:
        return BsonLogicalTypeAnnotation.create();
      case MAP_KEY_VALUE:
        return MapKeyValueTypeAnnotation.create();
      default:
        throw new RuntimeException("Can't convert original type to logical type, unknown original type " + originalType);
    }
  }

  class StringLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private static final StringLogicalTypeAnnotation INSTANCE = new StringLogicalTypeAnnotation();

    public static LogicalTypeAnnotation create() {
      return INSTANCE;
    }

    private StringLogicalTypeAnnotation() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.STRING(new StringType());
    }

    @Override
    public ConvertedType toConvertedType() {
      return ConvertedType.UTF8;
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.UTF8;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof StringLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class MapLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private static final MapLogicalTypeAnnotation INSTANCE = new MapLogicalTypeAnnotation();

    public static LogicalTypeAnnotation create() {
      return INSTANCE;
    }

    private MapLogicalTypeAnnotation() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.MAP(new MapType());
    }

    @Override
    public ConvertedType toConvertedType() {
      return ConvertedType.MAP;
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.MAP;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof MapLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class ListLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private static final ListLogicalTypeAnnotation INSTANCE = new ListLogicalTypeAnnotation();

    public static LogicalTypeAnnotation create() {
      return INSTANCE;
    }

    private ListLogicalTypeAnnotation() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.LIST(new ListType());
    }

    @Override
    public ConvertedType toConvertedType() {
      return ConvertedType.LIST;
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.LIST;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof ListLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class EnumLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private static final EnumLogicalTypeAnnotation INSTANCE = new EnumLogicalTypeAnnotation();

    public static LogicalTypeAnnotation create() {
      return INSTANCE;
    }

    private EnumLogicalTypeAnnotation() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.ENUM(new EnumType());
    }

    @Override
    public ConvertedType toConvertedType() {
      return ConvertedType.ENUM;
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.ENUM;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof EnumLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class DecimalLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private final int scale;
    private final int precision;

    public static LogicalTypeAnnotation create(int scale, int precision) {
      return new DecimalLogicalTypeAnnotation(scale, precision);
    }

    private DecimalLogicalTypeAnnotation(int scale, int precision) {
      this.scale = scale;
      this.precision = precision;
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.DECIMAL(new DecimalType(scale, precision));
    }

    @Override
    public ConvertedType toConvertedType() {
      return ConvertedType.DECIMAL;
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.DECIMAL;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DecimalLogicalTypeAnnotation)) {
        return false;
      }
      DecimalLogicalTypeAnnotation other = (DecimalLogicalTypeAnnotation) obj;
      return scale == other.scale && precision == other.precision;
    }

    @Override
    public int hashCode() {
      return Objects.hash(scale, precision);
    }
  }

  class DateLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private static final DateLogicalTypeAnnotation INSTANCE = new DateLogicalTypeAnnotation();

    public static LogicalTypeAnnotation create() {
      return INSTANCE;
    }

    private DateLogicalTypeAnnotation() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.DATE(new DateType());
    }

    @Override
    public ConvertedType toConvertedType() {
      return ConvertedType.DATE;
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.DATE;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof DateLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  enum TimeUnit {
    MILLIS,
    MICROS
  }

  static org.apache.parquet.format.TimeUnit convertUnit(TimeUnit unit) {
    switch (unit) {
      case MICROS:
        return org.apache.parquet.format.TimeUnit.MICROS(new MicroSeconds());
      case MILLIS:
        return org.apache.parquet.format.TimeUnit.MILLIS(new MilliSeconds());
      default:
        throw new RuntimeException("Unknown time unit " + unit);
    }
  }

  class TimeLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private final boolean isAdjustedToUTC;
    private final TimeUnit unit;

    public static LogicalTypeAnnotation create(boolean isAdjustedToUTC, TimeUnit unit) {
      return new TimeLogicalTypeAnnotation(isAdjustedToUTC, unit);
    }

    private TimeLogicalTypeAnnotation(boolean isAdjustedToUTC, TimeUnit unit) {
      this.isAdjustedToUTC = isAdjustedToUTC;
      this.unit = unit;
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.TIME(new TimeType(isAdjustedToUTC, convertUnit(unit)));
    }

    @Override
    public ConvertedType toConvertedType() {
      switch (toOriginalType()) {
        case TIME_MILLIS:
          return ConvertedType.TIME_MILLIS;
        case TIME_MICROS:
          return ConvertedType.TIME_MICROS;
        default:
          throw new RuntimeException("Unknown converted type for " + toOriginalType());
      }
    }

    @Override
    public OriginalType toOriginalType() {
      switch (unit) {
        case MILLIS:
          return OriginalType.TIME_MILLIS;
        case MICROS:
          return OriginalType.TIME_MICROS;
        default:
          throw new RuntimeException("Unknown original type for " + unit);
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TimeLogicalTypeAnnotation)) {
        return false;
      }
      TimeLogicalTypeAnnotation other = (TimeLogicalTypeAnnotation) obj;
      return isAdjustedToUTC == other.isAdjustedToUTC && unit == other.unit;
    }

    @Override
    public int hashCode() {
      return Objects.hash(isAdjustedToUTC, unit);
    }
  }

  class TimestampLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private final boolean isAdjustedToUTC;
    private final TimeUnit unit;

    public static LogicalTypeAnnotation create(boolean isAdjustedToUTC, TimeUnit unit) {
      return new TimestampLogicalTypeAnnotation(isAdjustedToUTC, unit);
    }

    private TimestampLogicalTypeAnnotation(boolean isAdjustedToUTC, TimeUnit unit) {
      this.isAdjustedToUTC = isAdjustedToUTC;
      this.unit = unit;
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.TIMESTAMP(new TimestampType(isAdjustedToUTC, convertUnit(unit)));
    }

    @Override
    public ConvertedType toConvertedType() {
      switch (toOriginalType()) {
        case TIMESTAMP_MICROS:
          return ConvertedType.TIMESTAMP_MICROS;
        case TIMESTAMP_MILLIS:
          return ConvertedType.TIMESTAMP_MILLIS;
        default:
          throw new RuntimeException("Unknown converted type for " + unit);
      }
    }

    @Override
    public OriginalType toOriginalType() {
      switch (unit) {
        case MILLIS:
          return OriginalType.TIMESTAMP_MILLIS;
        case MICROS:
          return OriginalType.TIMESTAMP_MICROS;
        default:
          throw new RuntimeException("Unknown original type for " + unit);
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TimestampLogicalTypeAnnotation)) {
        return false;
      }
      TimestampLogicalTypeAnnotation other = (TimestampLogicalTypeAnnotation) obj;
      return (isAdjustedToUTC == other.isAdjustedToUTC) && (unit == other.unit);
    }

    @Override
    public int hashCode() {
      return Objects.hash(isAdjustedToUTC, unit);
    }
  }

  class IntLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private final byte bitWidth;
    private final boolean isSigned;

    public static LogicalTypeAnnotation create(byte bitWidth, boolean isSigned) {
      return new IntLogicalTypeAnnotation(bitWidth, isSigned);
    }

    private IntLogicalTypeAnnotation(byte bitWidth, boolean isSigned) {
      this.bitWidth = bitWidth;
      this.isSigned = isSigned;
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.INTEGER(new IntType(bitWidth, isSigned));
    }

    @Override
    public ConvertedType toConvertedType() {
      switch (toOriginalType()) {
        case INT_8:
          return ConvertedType.INT_8;
        case INT_16:
          return ConvertedType.INT_16;
        case INT_32:
          return ConvertedType.INT_32;
        case INT_64:
          return ConvertedType.INT_64;
        case UINT_8:
          return ConvertedType.UINT_8;
        case UINT_16:
          return ConvertedType.UINT_16;
        case UINT_32:
          return ConvertedType.UINT_32;
        case UINT_64:
          return ConvertedType.UINT_64;
        default:
          throw new RuntimeException("Unknown original type " + toOriginalType());
      }
    }

    @Override
    public OriginalType toOriginalType() {
      switch (bitWidth) {
        case 8:
          return isSigned ? OriginalType.INT_8 : OriginalType.UINT_8;
        case 16:
          return isSigned ? OriginalType.INT_16 : OriginalType.UINT_16;
        case 32:
          return isSigned ? OriginalType.INT_32 : OriginalType.UINT_32;
        case 64:
          return isSigned ? OriginalType.INT_64 : OriginalType.UINT_64;
        default:
          throw new RuntimeException("Unknown original type " + toOriginalType());
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof IntLogicalTypeAnnotation)) {
        return false;
      }
      IntLogicalTypeAnnotation other = (IntLogicalTypeAnnotation) obj;
      return (bitWidth == other.bitWidth) && (isSigned == other.isSigned);
    }

    @Override
    public int hashCode() {
      return Objects.hash(bitWidth, isSigned);
    }
  }

  class JsonLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private static final JsonLogicalTypeAnnotation INSTANCE = new JsonLogicalTypeAnnotation();

    public static LogicalTypeAnnotation create() {
      return INSTANCE;
    }

    private JsonLogicalTypeAnnotation() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.JSON(new JsonType());
    }

    @Override
    public ConvertedType toConvertedType() {
      return ConvertedType.JSON;
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.JSON;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof JsonLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class BsonLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private static final BsonLogicalTypeAnnotation INSTANCE = new BsonLogicalTypeAnnotation();

    public static LogicalTypeAnnotation create() {
      return INSTANCE;
    }

    private BsonLogicalTypeAnnotation() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.BSON(new BsonType());
    }

    @Override
    public ConvertedType toConvertedType() {
      return ConvertedType.BSON;
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.BSON;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof BsonLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  // This logical type annotation is implemented to support backward compatibility with ConvertedType.
  // The new logical type representation in parquet-format doesn't have any interval type,
  // thus this annotation is mapped to UNKNOWN.
  class IntervalLogicalTypeAnnotation implements LogicalTypeAnnotation {
    private static IntervalLogicalTypeAnnotation INSTANCE = new IntervalLogicalTypeAnnotation();

    public static LogicalTypeAnnotation create() {
      return INSTANCE;
    }

    private IntervalLogicalTypeAnnotation() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.UNKNOWN(new NullType());
    }

    @Override
    public ConvertedType toConvertedType() {
      return ConvertedType.INTERVAL;
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.INTERVAL;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof IntervalLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  // This logical type annotation is implemented to support backward compatibility with ConvertedType.
  // The new logical type representation in parquet-format doesn't have any key-value type,
  // thus this annotation is mapped to UNKNOWN. This type shouldn't be used.
  class MapKeyValueTypeAnnotation implements LogicalTypeAnnotation {
    private static MapKeyValueTypeAnnotation INSTANCE = new MapKeyValueTypeAnnotation();

    public static LogicalTypeAnnotation create() {
      return INSTANCE;
    }

    private MapKeyValueTypeAnnotation() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.UNKNOWN(new NullType());
    }

    @Override
    public ConvertedType toConvertedType() {
      return ConvertedType.MAP_KEY_VALUE;
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.MAP_KEY_VALUE;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof MapKeyValueTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }
}
