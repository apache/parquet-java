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
package parquet.pojo.converter.map;

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
import java.util.Map;

/**
 * Converter for types that implement {@link Map}. Uses {@link ConcreteType} to resolve ambiguities about interfaces on left sides of field declarations
 *
 */
public class MapConverter extends GroupConverter implements PojoConverter {
  private final Container mapContainer = new Container();
  private final ConstructorAccess constructorAccess;
  private FieldAccessor parentFieldAccessor;
  private final Container parentContainer;
  private final Converter converter;

  public MapConverter(Class clazz, Field field, Container parentContainer, Class... genericArguments) {
    this.parentContainer = parentContainer;

    //represents the types for the key and value of the map, if we don't have a field to interrogate,
    //so the key type will be in [0] and the value in [1]
    if (genericArguments.length == 2) {
      Converter keyConverter = Resolver.newResolver(genericArguments[0], null, null).getConverter();
      Converter valueConverter = Resolver.newResolver(genericArguments[1], null, null).getConverter();

      converter = new MapValuesConverter(keyConverter, valueConverter, mapContainer);
      constructorAccess = ConstructorAccess.get(clazz);
    } else {
      Type type = field.getGenericType();

      if (type instanceof ParameterizedType) {
        //resolve the key and value classes
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Class keyClass = (Class) parameterizedType.getActualTypeArguments()[0];
        Class valueClass = (Class) parameterizedType.getActualTypeArguments()[1];

        Converter keyConverter = Resolver.newResolver(keyClass, null, null).getConverter();
        Converter valueConverter = Resolver.newResolver(valueClass, null, null).getConverter();

        converter = new MapValuesConverter(keyConverter, valueConverter, mapContainer);
        Class fieldType = field.getType();

        //if the left side of the declaration is a map, but not a concrete type, we need to probe the annotation for what to create it as
        if (fieldType == Map.class) {
          ConcreteType concreteType = field.getAnnotation(ConcreteType.class);

          if (concreteType == null) {
            throw new IllegalStateException(
              String.format(
                "Cannot create non-concrete maps without @ConcreteType annotation. Offending field: %s",
                field
              )
            );
          }

          constructorAccess = ConstructorAccess.get(concreteType.createAs());
        } else {
          constructorAccess = ConstructorAccess.get(fieldType);
        }

        parentFieldAccessor = new DelegatingFieldAccessor(field);
      } else {
        throw new IllegalStateException(String.format("Cannot use untyped maps. Offending field: %s", field));
      }
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converter;
  }

  @Override
  public void start() {
    mapContainer.set(constructorAccess.newInstance());
  }

  @Override
  public void end() {
    parentFieldAccessor.set(parentContainer.get(), mapContainer.get());
  }

  @Override
  public Object getValueAndReset() {
    return mapContainer.get();
  }

  /**
   * Converter for the repeated values of a map
   */
  static class MapValuesConverter extends GroupConverter implements PojoConverter {
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final Container<Map> mapContainer;
    private final PojoConverter keyConverterAsPojoConverter;
    private final PojoConverter valueConverterAsPojoConverter;

    public MapValuesConverter(Converter keyConverter, Converter valueConverter, Container<Map> mapContainer) {
      this.keyConverter = keyConverter;
      this.valueConverter = valueConverter;
      this.mapContainer = mapContainer;
      this.keyConverterAsPojoConverter = (PojoConverter) keyConverter;
      this.valueConverterAsPojoConverter = (PojoConverter) valueConverter;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex == 0) {
        return keyConverter;
      }

      if (fieldIndex == 1) {
        return valueConverter;
      }

      throw new IllegalStateException("Field index out of bound for map conversion.");
    }

    @Override
    public void start() {
    }

    @Override
    public void end() {
      mapContainer.get().put(
        keyConverterAsPojoConverter.getValueAndReset(), valueConverterAsPojoConverter.getValueAndReset()
      );
    }

    @Override
    public Object getValueAndReset() {
      throw new NotImplementedException();
    }
  }
}