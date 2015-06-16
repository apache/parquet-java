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

import org.apache.parquet.Preconditions;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;

import static org.apache.parquet.schema.OriginalType.*;

/**
 * Utility functions to convert from Java-like map and list types
 * to equivalent Parquet types.
 */
public abstract class ConversionPatterns {

  private static final String ELEMENT_NAME = "element";

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
   * @deprecated use listOfElements instead
   */
  @Deprecated
  public static GroupType listType(Repetition repetition, String alias, Type nestedType) {
    return listWrapper(
            repetition,
            alias,
            LIST,
            nestedType
    );
  }

  /**
   * Creates a 3-level list structure annotated with LIST with elements of the
   * given elementType. The repeated level is inserted automatically and the
   * elementType's repetition should be the correct repetition of the elements,
   * required for non-null and optional for nullable.
   *
   * @param listRepetition the repetition of the entire list structure
   * @param name the name of the list structure type
   * @param elementType the type of elements contained by the list
   * @return a GroupType that represents the list
   */
  public static GroupType listOfElements(Repetition listRepetition, String name, Type elementType) {
    Preconditions.checkArgument(elementType.getName().equals(ELEMENT_NAME),
        "List element type must be named 'element'");
    return listWrapper(
        listRepetition,
        name,
        LIST,
        new GroupType(Repetition.REPEATED, "list", elementType)
    );
  }
}
