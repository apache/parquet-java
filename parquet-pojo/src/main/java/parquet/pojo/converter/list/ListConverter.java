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
package parquet.pojo.converter.list;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import org.apache.commons.lang.NotImplementedException;
import parquet.hadoop.mapred.Container;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.pojo.ConcreteType;
import parquet.pojo.Resolver;
import parquet.pojo.converter.PojoConverter;
import parquet.pojo.field.DelegatingFieldAccessor;
import parquet.pojo.field.FieldAccessor;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Converter for types that implement {@link List}
 *
 * @author Jason Ruckman
 */
public class ListConverter extends GroupConverter implements PojoConverter {
  private final Container listContainer = new Container();
  private final ConstructorAccess constructorAccess;
  private final Converter converter;
  private FieldAccessor parentFieldAccessor;
  private final Container parentContainer;

  public ListConverter(Class clazz, Field field, Container parentContainer, Class... genericArguments) {
    this.parentContainer = parentContainer;
    Class valueClass;
    Class fieldType;
    if (genericArguments.length == 1) {
      valueClass = genericArguments[0];
      fieldType = clazz;
    } else {
      Type type = field.getGenericType();
      fieldType = field.getType();
      this.parentFieldAccessor = new DelegatingFieldAccessor(field);
      if (type instanceof ParameterizedType) {
        ParameterizedType parameterizedType = (ParameterizedType) type;
        valueClass = (Class) parameterizedType.getActualTypeArguments()[0];
      } else {
        throw new IllegalStateException(String.format("Cannot use untyped lists. Offending field: %s", field));
      }
    }

    if (fieldType == List.class) {
      ConcreteType concreteType = field.getAnnotation(ConcreteType.class);

      if (concreteType == null) {
        throw new IllegalStateException(
          String.format(
            "Cannot create non-concrete lists without @ConcreteType annotation. Offending field: %s", field
          )
        );
      }

      constructorAccess = ConstructorAccess.get(concreteType.createAs());
    } else {
      constructorAccess = ConstructorAccess.get(fieldType);
    }

    Converter unwrappedConverter = Resolver.newResolver(valueClass, null, null).getConverter();

    converter = new ListValuesConverter(unwrappedConverter, listContainer);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converter;
  }

  @Override
  public void start() {
    listContainer.set(constructorAccess.newInstance());
  }

  @Override
  public void end() {
    parentFieldAccessor.set(parentContainer.get(), listContainer.get());
  }

  @Override
  public Object getRawValue() {
    return listContainer.get();
  }

  /**
   * Converter for the repeated values of a list
   */
  static class ListValuesConverter extends GroupConverter implements PojoConverter {
    private final Converter delegateConverter;
    private final PojoConverter asPojoConverter;
    private final Container<List> listContainer;

    public ListValuesConverter(Converter delegateConverter, Container<List> listContainer) {
      this.delegateConverter = delegateConverter;
      this.listContainer = listContainer;
      this.asPojoConverter = (PojoConverter) delegateConverter;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return delegateConverter;
    }

    @Override
    public void start() {

    }

    @Override
    public void end() {
      listContainer.get().add(asPojoConverter.getRawValue());
    }

    @Override
    public Object getRawValue() {
      throw new NotImplementedException();
    }
  }
}