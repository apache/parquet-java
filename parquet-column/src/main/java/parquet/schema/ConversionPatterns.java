/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.schema;

import static parquet.schema.OriginalType.*;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

/**
 * Utility functions to convert from Java-like map and list types
 * to equivalent Parquet types.
 * 
 * TODO(julien): this class appears to be unused. Is it dead code?
 */
public abstract class ConversionPatterns {
  /**
   * to preserve the difference between empty list and null
   * @param alias
   * @param originalType
   * @param groupType
   * @return an optional group
   */
  private static GroupType listWrapper(Repetition repetition, String alias, OriginalType originalType, GroupType groupType) {
    return new GroupType(repetition, alias, originalType, groupType);
  }

  public static GroupType mapType(Repetition repetition, String alias, Type valueType) {
    if (!valueType.getName().equals("value")) {
      throw new RuntimeException(valueType.getName() + " should be value");
    }
    return listWrapper(
        repetition,
        alias,
        MAP,
        new GroupType(
            Repetition.REPEATED,
            "map",
            MAP_KEY_VALUE,
            new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "key"),
            valueType)
        );
  }

  public static GroupType listType(Repetition repetition, String alias, Type nestedType) {
    GroupType repeatedField;
    if (nestedType.isPrimitive()) {
      repeatedField = new GroupType(
          Repetition.REPEATED,
          "bag",
          nestedType);
    } else {
      final GroupType nestedGroupType = nestedType.asGroupType();
      repeatedField = new GroupType(
          Repetition.REPEATED,
          nestedGroupType.getName(),
          nestedGroupType.getFields());
    }
    return listWrapper(
        repetition,
        alias,
        LIST,
        repeatedField
        );
  }
}
