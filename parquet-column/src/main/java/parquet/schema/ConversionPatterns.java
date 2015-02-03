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
package parquet.schema;

import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

import static parquet.schema.OriginalType.*;

/**
 * Utility functions to convert from Java-like map and list types
 * to equivalent Parquet types.
 */
public abstract class ConversionPatterns {
  /**
   * to preserve the difference between empty list and null when optional
   *
   * @param repetition
   * @param alias        name of the field
   * @param originalType
   * @param nested       the nested repeated field
   * @return a group type
   */
  private static GroupType listWrapper(Repetition repetition, String alias, OriginalType originalType, Type nested) {
    if (!nested.isRepetition(Repetition.REPEATED)) {
      throw new IllegalArgumentException("Nested type should be repeated: " + nested);
    }
    return new GroupType(repetition, alias, originalType, nested);
  }

  public static GroupType mapType(Repetition repetition, String alias, Type keyType, Type valueType) {
    return mapType(repetition, alias, "map", keyType, valueType);
  }

  public static GroupType stringKeyMapType(Repetition repetition, String alias, String mapAlias, Type valueType) {
    return mapType(repetition, alias, mapAlias, new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "key", OriginalType.UTF8), valueType);
  }

  public static GroupType stringKeyMapType(Repetition repetition, String alias, Type valueType) {
    return stringKeyMapType(repetition, alias, "map", valueType);
  }

  public static GroupType mapType(Repetition repetition, String alias, String mapAlias, Type keyType, Type valueType) {
    //support projection only on key of a map
    if (valueType == null) {
      return listWrapper(
              repetition,
              alias,
              MAP,
              new GroupType(
                      Repetition.REPEATED,
                      mapAlias,
                      MAP_KEY_VALUE,
                      keyType)
      );
    } else {
      if (!valueType.getName().equals("value")) {
        throw new RuntimeException(valueType.getName() + " should be value");
      }
      return listWrapper(
              repetition,
              alias,
              MAP,
              new GroupType(
                      Repetition.REPEATED,
                      mapAlias,
                      MAP_KEY_VALUE,
                      keyType,
                      valueType)
      );
    }
  }

  /**
   * @param repetition
   * @param alias      name of the field
   * @param nestedType
   * @return
   */
  public static GroupType listType(Repetition repetition, String alias, Type nestedType) {
    return listWrapper(
            repetition,
            alias,
            LIST,
            nestedType
    );
  }
}
