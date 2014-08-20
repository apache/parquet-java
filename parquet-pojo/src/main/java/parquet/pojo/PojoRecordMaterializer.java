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

import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.pojo.converter.PojoConverter;
import parquet.pojo.converter.PrimitiveRootConverter;
import parquet.pojo.field.FieldUtils;

public class PojoRecordMaterializer extends RecordMaterializer {
  private final GroupConverter rootConverter;
  private final PojoConverter pojoConverter;

  public PojoRecordMaterializer(Class clazz, Class... genericArguments) {
    if (FieldUtils.isConsideredPrimitive(clazz)) {
      rootConverter = new PrimitiveRootConverter(clazz);
    } else {
      rootConverter = (GroupConverter) Resolver.newResolver(clazz, null, genericArguments).getConverter();
    }
    pojoConverter = (PojoConverter) rootConverter;
  }

  @Override
  public Object getCurrentRecord() {
    return pojoConverter.getValueAndReset();
  }

  @Override
  public GroupConverter getRootConverter() {
    return rootConverter;
  }
}