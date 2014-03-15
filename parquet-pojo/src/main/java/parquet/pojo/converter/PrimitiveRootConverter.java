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
package parquet.pojo.converter;

import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.pojo.Resolver;

/**
 * {@link GroupConverter} wrapper for primitive converters so they can be used at the top level
 *
 * @author Jason Ruckman https://github.com/JasonRuckman
 */
public class PrimitiveRootConverter extends GroupConverter implements PojoConverter {
  private final Converter converter;
  private final PojoConverter pojoConverter;
  private Object currentObject;

  public PrimitiveRootConverter(Class clazz) {
    converter = Resolver.newResolver(clazz, null).getConverter();
    pojoConverter = (PojoConverter) converter;
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converter;
  }

  @Override
  public void start() {
  }

  @Override
  public void end() {
    currentObject = pojoConverter.getRawValue();
  }

  @Override
  public Object getRawValue() {
    return currentObject;
  }
}
