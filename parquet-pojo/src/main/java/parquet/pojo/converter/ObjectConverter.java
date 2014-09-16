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

import com.esotericsoftware.reflectasm.ConstructorAccess;
import parquet.hadoop.mapred.Container;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.pojo.Resolver;
import parquet.pojo.field.DelegatingFieldAccessor;
import parquet.pojo.field.FieldAccessor;
import parquet.pojo.field.FieldUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * A converter that is used for non-primitive / array / map or list types
 *
 */
public class ObjectConverter extends GroupConverter implements PojoConverter {
  private final Class clazz;
  private final Converter[] converters;
  private final ConstructorAccess constructorAccess;
  private final Container thisInstanceContainer = new Container();
  private final Container parentInstanceContainer;
  private FieldAccessor parentFieldAccessorIfPresent;

  public ObjectConverter(
    Class clazz,
    Container parentInstanceContainer,
    Field fieldIfPresent
  ) {
    this.clazz = clazz;
    this.parentInstanceContainer = parentInstanceContainer;

    if (fieldIfPresent != null) {
      this.parentFieldAccessorIfPresent = new DelegatingFieldAccessor(fieldIfPresent);
    }

    List<Field> fields = FieldUtils.getAllFields(clazz);

    this.converters = new Converter[fields.size()];
    this.constructorAccess = ConstructorAccess.get(this.clazz);

    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      converters[i] = Resolver.newResolver(field.getType(), field, new Class[0], thisInstanceContainer).getConverter();
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    thisInstanceContainer.set(constructorAccess.newInstance());
  }

  @Override
  public void end() {
    parentFieldAccessorIfPresent.set(parentInstanceContainer.get(), thisInstanceContainer.get());
  }

  @Override
  public Object getValueAndReset() {
    return thisInstanceContainer.get();
  }
}
