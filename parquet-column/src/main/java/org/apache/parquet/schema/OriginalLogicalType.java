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

import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.format.BsonType;
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

public interface OriginalLogicalType {
  /**
   * Convert this parquet-mr logical type to parquet-format LogicalType.
   *
   * @return the parquet-format representation of this logical type implementation
   */
  LogicalType toLogicalType();

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
  static OriginalLogicalType fromOriginalType(OriginalType originalType) {
    if (originalType == null) {
      return null;
    }
    switch (originalType) {
      case UTF8:
        return OriginalLogicalType.StringLogicalType.create();
      case MAP:
        return OriginalLogicalType.MapLogicalType.create();
      case DECIMAL:
        return OriginalLogicalType.DecimalLogicalType.create();
      case LIST:
        return OriginalLogicalType.ListLogicalType.create();
      case DATE:
        return OriginalLogicalType.DateLogicalType.create();
      case INTERVAL:
        return OriginalLogicalType.IntervalLogicalType.create();
      case TIMESTAMP_MILLIS:
        return OriginalLogicalType.TimestampLogicalType.create(true, OriginalLogicalType.TimeUnit.MILLIS);
      case TIMESTAMP_MICROS:
        return OriginalLogicalType.TimestampLogicalType.create(true, OriginalLogicalType.TimeUnit.MICROS);
      case TIME_MILLIS:
        return OriginalLogicalType.TimeLogicalType.create(true, OriginalLogicalType.TimeUnit.MILLIS);
      case TIME_MICROS:
        return OriginalLogicalType.TimeLogicalType.create(true, OriginalLogicalType.TimeUnit.MICROS);
      case UINT_8:
        return OriginalLogicalType.IntLogicalType.create((byte) 8, false);
      case UINT_16:
        return OriginalLogicalType.IntLogicalType.create((byte) 16, false);
      case UINT_32:
        return OriginalLogicalType.IntLogicalType.create((byte) 32, false);
      case UINT_64:
        return OriginalLogicalType.IntLogicalType.create((byte) 64, false);
      case INT_8:
        return OriginalLogicalType.IntLogicalType.create((byte) 8, true);
      case INT_16:
        return OriginalLogicalType.IntLogicalType.create((byte) 16, true);
      case INT_32:
        return OriginalLogicalType.IntLogicalType.create((byte) 32, true);
      case INT_64:
        return OriginalLogicalType.IntLogicalType.create((byte) 64, true);
      case ENUM:
        return OriginalLogicalType.EnumLogicalType.create();
      case JSON:
        return OriginalLogicalType.JsonLogicalType.create();
      case BSON:
        return OriginalLogicalType.BsonLogicalType.create();
      case MAP_KEY_VALUE:
        return OriginalLogicalType.MapKeyValueType.create();
      default:
        return OriginalLogicalType.NullLogicalType.create();
    }
  }

  class StringLogicalType implements OriginalLogicalType {
    private static final StringLogicalType INSTANCE = new StringLogicalType();

    public static OriginalLogicalType create() {
      return INSTANCE;
    }

    private StringLogicalType() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.STRING(new StringType());
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.UTF8;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof StringLogicalType;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class MapLogicalType implements OriginalLogicalType {
    private static final MapLogicalType INSTANCE = new MapLogicalType();

    public static OriginalLogicalType create() {
      return INSTANCE;
    }

    private MapLogicalType() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.MAP(new MapType());
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.MAP;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof MapLogicalType;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class ListLogicalType implements OriginalLogicalType {
    private static final ListLogicalType INSTANCE = new ListLogicalType();

    public static OriginalLogicalType create() {
      return INSTANCE;
    }

    private ListLogicalType() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.LIST(new ListType());
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.LIST;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof ListLogicalType;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class EnumLogicalType implements OriginalLogicalType {
    private static final EnumLogicalType INSTANCE = new EnumLogicalType();

    public static OriginalLogicalType create() {
      return INSTANCE;
    }

    private EnumLogicalType() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.ENUM(new EnumType());
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.ENUM;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof EnumLogicalType;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class DecimalLogicalType implements OriginalLogicalType {

    private int scale;
    private int precision;

    public static OriginalLogicalType create() {
      return new DecimalLogicalType(0, 0);
    }

    public static OriginalLogicalType create(int scale, int precision) {
      return new DecimalLogicalType(scale, precision);
    }

    private DecimalLogicalType(int scale, int precision) {
      this.scale = scale;
      this.precision = precision;
    }

    public void setPrecision(int precision) {
      this.precision = precision;
    }

    public void setScale(int scale) {
      this.scale = scale;
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.DECIMAL(new DecimalType(scale, precision));
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.DECIMAL;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DecimalLogicalType)) {
        return false;
      }
      DecimalLogicalType other = (DecimalLogicalType) obj;
      return scale == other.scale && precision == other.precision;
    }

    @Override
    public int hashCode() {
      return Objects.hash(scale, precision);
    }
  }

  class DateLogicalType implements OriginalLogicalType {
    private static final DateLogicalType INSTANCE = new DateLogicalType();

    public static OriginalLogicalType create() {
      return INSTANCE;
    }

    private DateLogicalType() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.DATE(new DateType());
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.DATE;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof DateLogicalType;
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
        throw new ShouldNeverHappenException();
    }
  }

  class TimeLogicalType implements OriginalLogicalType {
    private final boolean isAdjustedToUTC;
    private final TimeUnit unit;

    public static OriginalLogicalType create(boolean isAdjustedToUTC, TimeUnit unit) {
      return new TimeLogicalType(isAdjustedToUTC, unit);
    }

    private TimeLogicalType(boolean isAdjustedToUTC, TimeUnit unit) {
      this.isAdjustedToUTC = isAdjustedToUTC;
      this.unit = unit;
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.TIME(new TimeType(isAdjustedToUTC, convertUnit(unit)));
    }

    @Override
    public OriginalType toOriginalType() {
      switch (unit) {
        case MILLIS:
          return OriginalType.TIME_MILLIS;
        case MICROS:
          return OriginalType.TIME_MICROS;
      }

      throw new ShouldNeverHappenException();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TimeLogicalType)) {
        return false;
      }
      TimeLogicalType other = (TimeLogicalType) obj;
      return isAdjustedToUTC == other.isAdjustedToUTC && unit == other.unit;
    }

    @Override
    public int hashCode() {
      return Objects.hash(isAdjustedToUTC, unit);
    }
  }

  class TimestampLogicalType implements OriginalLogicalType {
    private final boolean isAdjustedToUTC;
    private final TimeUnit unit;

    public static OriginalLogicalType create(boolean isAdjustedToUTC, TimeUnit unit) {
      return new TimestampLogicalType(isAdjustedToUTC, unit);
    }

    private TimestampLogicalType(boolean isAdjustedToUTC, TimeUnit unit) {
      this.isAdjustedToUTC = isAdjustedToUTC;
      this.unit = unit;
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.TIMESTAMP(new TimestampType(isAdjustedToUTC, convertUnit(unit)));
    }

    @Override
    public OriginalType toOriginalType() {
      switch (unit) {
        case MILLIS:
          return OriginalType.TIMESTAMP_MILLIS;
        case MICROS:
          return OriginalType.TIMESTAMP_MICROS;
        default:
          throw new ShouldNeverHappenException();
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TimestampLogicalType)) {
        return false;
      }
      TimestampLogicalType other = (TimestampLogicalType) obj;
      return (isAdjustedToUTC == other.isAdjustedToUTC) && (unit == other.unit);
    }

    @Override
    public int hashCode() {
      return Objects.hash(isAdjustedToUTC, unit);
    }
  }

  class IntLogicalType implements OriginalLogicalType {
    private final byte bitWidth;
    private final boolean isSigned;

    public static OriginalLogicalType create(byte bitWidth, boolean isSigned) {
      return new IntLogicalType(bitWidth, isSigned);
    }

    private IntLogicalType(byte bitWidth, boolean isSigned) {
      this.bitWidth = bitWidth;
      this.isSigned = isSigned;
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.INTEGER(new IntType(bitWidth, isSigned));
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
          throw new ShouldNeverHappenException();
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof IntLogicalType)) {
        return false;
      }
      IntLogicalType other = (IntLogicalType) obj;
      return (bitWidth == other.bitWidth) && (isSigned == other.isSigned);
    }

    @Override
    public int hashCode() {
      return Objects.hash(bitWidth, isSigned);
    }
  }

  class NullLogicalType implements OriginalLogicalType {
    private static final NullLogicalType INSTANCE = new NullLogicalType();

    public static OriginalLogicalType create() {
      return INSTANCE;
    }

    private NullLogicalType() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.UNKNOWN(new NullType());
    }

    @Override
    public OriginalType toOriginalType() {
      return null;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof NullLogicalType;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class JsonLogicalType implements OriginalLogicalType {
    private static final JsonLogicalType INSTANCE = new JsonLogicalType();

    public static OriginalLogicalType create() {
      return INSTANCE;
    }

    private JsonLogicalType() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.JSON(new JsonType());
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.JSON;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof JsonLogicalType;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class BsonLogicalType implements OriginalLogicalType {
    private static final BsonLogicalType INSTANCE = new BsonLogicalType();

    public static OriginalLogicalType create() {
      return INSTANCE;
    }

    private BsonLogicalType() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.BSON(new BsonType());
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.BSON;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof BsonLogicalType;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class IntervalLogicalType implements OriginalLogicalType {
    private static IntervalLogicalType INSTANCE = new IntervalLogicalType();

    public static OriginalLogicalType create() {
      return INSTANCE;
    }

    private IntervalLogicalType() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.UNKNOWN(new NullType());
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.INTERVAL;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof IntervalLogicalType;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }

  class MapKeyValueType implements OriginalLogicalType {
    private static MapKeyValueType INSTANCE = new MapKeyValueType();

    public static OriginalLogicalType create() {
      return INSTANCE;
    }

    private MapKeyValueType() {
    }

    @Override
    public LogicalType toLogicalType() {
      return LogicalType.UNKNOWN(new NullType());
    }

    @Override
    public OriginalType toOriginalType() {
      return OriginalType.MAP_KEY_VALUE;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof MapKeyValueType;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus use class hashcode
      return getClass().hashCode();
    }
  }
}
