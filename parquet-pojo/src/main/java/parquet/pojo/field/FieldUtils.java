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
package parquet.pojo.field;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for resolving classes and fields
 *
 * @author Jason Ruckman https://github.com/JasonRuckman
 */
public class FieldUtils {
  /**
   * Returns fields for a {@link Class} and all its super types. Ignores transient or static fields. Fields from the top of the object hierarchy will be added first
   *
   * @param type
   * @return list of valid fields
   */
  public static List<Field> getAllFields(Class type) {
    List<Field> fields = new ArrayList<Field>();

    if (isConsideredPrimitive(type)) {
      return fields;
    }

    Field[] declaredFields = type.getDeclaredFields();

    if (type.getSuperclass() != null) {
      fields.addAll(getAllFields(type.getSuperclass()));
    }

    for (Field f : declaredFields) {
      if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers())) {
        continue;
      }
      fields.add(f);
    }

    return fields;
  }

  public static boolean isMap(Class clazz) {
    return Map.class.isAssignableFrom(clazz);
  }

  public static boolean isList(Class clazz) {
    return List.class.isAssignableFrom(clazz);
  }

  /**
   * Tests if a class fits a definition of primitive, defined as it is either a java primitive or its reference variant, a {@link String} a byte[] or an {@link Enum}
   *
   * @param type
   * @return true if the provided type is considered primitive
   */
  public static boolean isConsideredPrimitive(Class type) {
    return
      typeIsPrimitiveOrReferenceBoolean(type) ||
        typeIsPrimitiveOrReferenceByte(type) ||
        typeIsPrimitiveOrReferenceChar(type) ||
        typeIsPrimitiveOrReferenceShort(type) ||
        typeIsPrimitiveOrReferenceInt(type) ||
        typeIsPrimitiveOrReferenceFloat(type) ||
        typeIsPrimitiveOrReferenceLong(type) ||
        typeIsPrimitiveOrReferenceDouble(type) || type == byte[].class || type == String.class || type.isEnum();
  }

  private static boolean typeIsPrimitiveOrReferenceBoolean(Class clazz) {
    return clazz == boolean.class || clazz == Boolean.class;
  }

  private static boolean typeIsPrimitiveOrReferenceByte(Class clazz) {
    return clazz == byte.class || clazz == Byte.class;
  }

  private static boolean typeIsPrimitiveOrReferenceChar(Class clazz) {
    return clazz == char.class || clazz == Character.class;
  }

  private static boolean typeIsPrimitiveOrReferenceShort(Class clazz) {
    return clazz == short.class || clazz == Short.class;
  }

  private static boolean typeIsPrimitiveOrReferenceInt(Class clazz) {
    return clazz == int.class || clazz == Integer.class;
  }

  private static boolean typeIsPrimitiveOrReferenceLong(Class clazz) {
    return clazz == long.class || clazz == Long.class;
  }

  private static boolean typeIsPrimitiveOrReferenceFloat(Class clazz) {
    return clazz == float.class || clazz == Float.class;
  }

  private static boolean typeIsPrimitiveOrReferenceDouble(Class clazz) {
    return clazz == double.class || clazz == Double.class;
  }
}