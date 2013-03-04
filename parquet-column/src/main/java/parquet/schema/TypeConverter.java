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

import java.util.List;

/**
 * to convert a MessageType tree
 * @see Type#convert(TypeConverter)
 *
 * @author Julien Le Dem
 *
 * @param <T> the resulting Type
 */
public interface TypeConverter<T> {

  /**
   * @param path the path to that node
   * @param primitiveType the type to convert
   * @return the result of conversion
   */
  T convertPrimitiveType(List<GroupType> path, PrimitiveType primitiveType);

  /**
   * @param path the path to that node
   * @param groupType the type to convert
   * @param children its children already converted
   * @return the result of conversion
   */
  T convertGroupType(List<GroupType> path, GroupType groupType, List<T> children);

  /**
   * @param messageType the type to convert
   * @param children its children already converted
   * @return the result of conversion
   */
  T convertMessageType(MessageType messageType, List<T> children);

}
