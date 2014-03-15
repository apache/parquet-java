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
package parquet.pojo;

public class PojoUtils {

  /**
   * Utility method for resolving classes to name
   *
   * @param name
   * @return
   * @throws ClassNotFoundException if the class doesn't exist
   */
  public static Class classForNameWithPrimitiveSupport(String name) throws ClassNotFoundException {
    if (name.equals(boolean.class.getName())) {
      return boolean.class;
    } else if (name.equals(byte.class.getName())) {
      return byte.class;
    } else if (name.equals(short.class.getName())) {
      return short.class;
    } else if (name.equals(int.class.getName())) {
      return int.class;
    } else if (name.equals(char.class.getName())) {
      return char.class;
    } else if (name.equals(long.class.getName())) {
      return long.class;
    } else if (name.equals(float.class.getName())) {
      return float.class;
    } else if (name.equals(double.class.getName())) {
      return double.class;
    } else {
      return Class.forName(name);
    }
  }
}
