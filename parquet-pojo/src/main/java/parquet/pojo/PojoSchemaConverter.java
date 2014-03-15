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

import parquet.schema.MessageType;
import parquet.schema.Type;

import java.lang.reflect.Field;

/**
 *
 */
public class PojoSchemaConverter {
  /**
   * Creates a {@link MessageType} from the given {@code clazz}
   *
   * @param clazz
   * @param fieldIfPresent
   * @param genericArgumentsIfPresent
   * @return
   */
  public MessageType convert(Class clazz, Field fieldIfPresent, Class... genericArgumentsIfPresent) {
    Type type = new Resolver(clazz, fieldIfPresent, genericArgumentsIfPresent, null).getType();

    if (!type.isPrimitive()) {
      return new MessageType("message", type.asGroupType().getFields());
    }

    return new MessageType(
      "message", type
    );
  }
}
