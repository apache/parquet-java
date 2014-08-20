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

import java.util.HashMap;
import java.util.Map;

public class PojoUtils {
  private static final Map<String, Class> namesToClasses = new HashMap<String, Class>();

  static {
    namesToClasses.put(boolean.class.getName(), boolean.class);
    namesToClasses.put(byte.class.getName(), byte.class);
    namesToClasses.put(short.class.getName(), short.class);
    namesToClasses.put(int.class.getName(), int.class);
    namesToClasses.put(char.class.getName(), char.class);
    namesToClasses.put(long.class.getName(), long.class);
    namesToClasses.put(float.class.getName(), float.class);
    namesToClasses.put(double.class.getName(), double.class);
  }
  /**
   * Utility method for resolving classes to name
   *
   * @param name
   * @return
   * @throws ClassNotFoundException if the class doesn't exist
   */
  public static Class classForNameWithPrimitiveSupport(String name) throws ClassNotFoundException {
    Class clazz = namesToClasses.get(name);
    if(clazz != null) {
      return clazz;
    }
    return Class.forName(name);
  }
}