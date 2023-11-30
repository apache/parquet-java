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

package org.apache.parquet.format;

/**
 * Convenience instances of logical type classes.
 */
public class LogicalTypes {
  public static class TimeUnits {
    public static final TimeUnit MILLIS = TimeUnit.MILLIS(new MilliSeconds());
    public static final TimeUnit MICROS = TimeUnit.MICROS(new MicroSeconds());
  }

  public static LogicalType DECIMAL(int scale, int precision) {
    return LogicalType.DECIMAL(new DecimalType(scale, precision));
  }

  public static final LogicalType UTF8 = LogicalType.STRING(new StringType());
  public static final LogicalType MAP = LogicalType.MAP(new MapType());
  public static final LogicalType LIST = LogicalType.LIST(new ListType());
  public static final LogicalType ENUM = LogicalType.ENUM(new EnumType());
  public static final LogicalType DATE = LogicalType.DATE(new DateType());
  public static final LogicalType TIME_MILLIS = LogicalType.TIME(new TimeType(true, TimeUnits.MILLIS));
  public static final LogicalType TIME_MICROS = LogicalType.TIME(new TimeType(true, TimeUnits.MICROS));
  public static final LogicalType TIMESTAMP_MILLIS = LogicalType.TIMESTAMP(new TimestampType(true, TimeUnits.MILLIS));
  public static final LogicalType TIMESTAMP_MICROS = LogicalType.TIMESTAMP(new TimestampType(true, TimeUnits.MICROS));
  public static final LogicalType INT_8 = LogicalType.INTEGER(new IntType((byte) 8, true));
  public static final LogicalType INT_16 = LogicalType.INTEGER(new IntType((byte) 16, true));
  public static final LogicalType INT_32 = LogicalType.INTEGER(new IntType((byte) 32, true));
  public static final LogicalType INT_64 = LogicalType.INTEGER(new IntType((byte) 64, true));
  public static final LogicalType UINT_8 = LogicalType.INTEGER(new IntType((byte) 8, false));
  public static final LogicalType UINT_16 = LogicalType.INTEGER(new IntType((byte) 16, false));
  public static final LogicalType UINT_32 = LogicalType.INTEGER(new IntType((byte) 32, false));
  public static final LogicalType UINT_64 = LogicalType.INTEGER(new IntType((byte) 64, false));
  public static final LogicalType UNKNOWN = LogicalType.UNKNOWN(new NullType());
  public static final LogicalType JSON = LogicalType.JSON(new JsonType());
  public static final LogicalType BSON = LogicalType.BSON(new BsonType());
}
