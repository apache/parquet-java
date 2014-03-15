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
package parquet.pojo.converter.array;

import parquet.io.api.PrimitiveConverter;

import java.lang.reflect.Array;

/**
 * Converter whos sole job is to read a integer value and create a array of that size for the next converter to use
 *
 * @author Jason Ruckman https://github.com/JasonRuckman
 */
public class ArrayInstantiatingConverter extends PrimitiveConverter {
  private final Class componentType;
  private Object currentArray;

  public ArrayInstantiatingConverter(Class componentType) {
    this.componentType = componentType;
  }

  @Override
  public void addInt(int value) {
    currentArray = Array.newInstance(componentType, value);
  }

  public Object currentArray() {
    return currentArray;
  }
}
