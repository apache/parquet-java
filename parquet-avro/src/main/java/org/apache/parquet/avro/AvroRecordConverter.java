/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.avro;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.chars.CharArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

class AvroRecordConverter<T> extends AvroConverters.AvroGroupConverter {

  protected T currentRecord;
  private final Converter[] converters;

  private final Schema avroSchema;

  private final GenericData model;
  private final Map<Schema.Field, Object> recordDefaults = new HashMap<Schema.Field, Object>();

  public AvroRecordConverter(MessageType parquetSchema, Schema avroSchema,
                             GenericData baseModel) {
    this(null, parquetSchema, avroSchema, baseModel);
  }

  public AvroRecordConverter(ParentValueContainer parent,
                             GroupType parquetSchema, Schema avroSchema,
                             GenericData model) {
    super(parent);
    this.avroSchema = avroSchema;
    this.model = (model == null ? ReflectData.get() : model);
    this.converters = new Converter[parquetSchema.getFieldCount()];

    Map<String, Integer> avroFieldIndexes = new HashMap<String, Integer>();
    int avroFieldIndex = 0;
    for (Schema.Field field: avroSchema.getFields()) {
        avroFieldIndexes.put(field.name(), avroFieldIndex++);
    }

    int parquetFieldIndex = 0;
    for (Type parquetField: parquetSchema.getFields()) {
      final Schema.Field avroField = getAvroField(parquetField.getName());
      Schema nonNullSchema = AvroSchemaConverter.getNonNull(avroField.schema());
      final int finalAvroIndex = avroFieldIndexes.remove(avroField.name());
      converters[parquetFieldIndex++] = newConverter(
          nonNullSchema, parquetField, this.model, new ParentValueContainer() {
        @Override
        public void add(Object value) {
          AvroRecordConverter.this.set(avroField.name(), finalAvroIndex, value);
        }
      });
    }

    // store defaults for any new Avro fields from avroSchema that are not in
    // the writer schema (parquetSchema)
    for (String fieldName : avroFieldIndexes.keySet()) {
      Schema.Field field = avroSchema.getField(fieldName);
      if (field.schema().getType() == Schema.Type.NULL) {
        continue; // skip null since Parquet does not write nulls
      }
      if (field.defaultValue() == null || this.model.getDefaultValue(field) == null) {
        continue; // field has no default
      }
      // use this.model because model may be null
      recordDefaults.put(field, this.model.getDefaultValue(field));
    }
  }

  private Schema.Field getAvroField(String parquetFieldName) {
    Schema.Field avroField = avroSchema.getField(parquetFieldName);
    if (avroField != null) {
      return avroField;
    }

    for (Schema.Field f : avroSchema.getFields()) {
      if (f.aliases().contains(parquetFieldName)) {
        return f;
      }
    }

    throw new InvalidRecordException(String.format(
        "Parquet/Avro schema mismatch: Avro field '%s' not found",
        parquetFieldName));
  }

  private static Converter newConverter(Schema schema, Type type,
      GenericData model, ParentValueContainer parent) {
    if (schema.getType().equals(Schema.Type.BOOLEAN)) {
      return new AvroConverters.FieldBooleanConverter(parent);
    } else if (schema.getType().equals(Schema.Type.INT)) {
      Class<?> datumClass = getDatumClass(schema, model);
      if (datumClass == null) {
        return new AvroConverters.FieldIntegerConverter(parent);
      } else if (datumClass == byte.class || datumClass == Byte.class) {
        return new AvroConverters.FieldByteConverter(parent);
      } else if (datumClass == short.class || datumClass == Short.class) {
        return new AvroConverters.FieldShortConverter(parent);
      } else if (datumClass == char.class || datumClass == Character.class) {
        return new AvroConverters.FieldCharConverter(parent);
      }
      return new AvroConverters.FieldIntegerConverter(parent);
    } else if (schema.getType().equals(Schema.Type.LONG)) {
      return new AvroConverters.FieldLongConverter(parent);
    } else if (schema.getType().equals(Schema.Type.FLOAT)) {
      return new AvroConverters.FieldFloatConverter(parent);
    } else if (schema.getType().equals(Schema.Type.DOUBLE)) {
      return new AvroConverters.FieldDoubleConverter(parent);
    } else if (schema.getType().equals(Schema.Type.BYTES)) {
      Class<?> datumClass = getDatumClass(schema, model);
      if (datumClass == null) {
        return new AvroConverters.FieldByteBufferConverter(parent);
      } else if (datumClass.isArray() && datumClass.getComponentType() == byte.class) {
        return new AvroConverters.FieldByteArrayConverter(parent);
      }
      return new AvroConverters.FieldByteBufferConverter(parent);
    } else if (schema.getType().equals(Schema.Type.STRING)) {
      return new AvroConverters.FieldStringConverter(parent);
    } else if (schema.getType().equals(Schema.Type.RECORD)) {
      return new AvroRecordConverter(parent, type.asGroupType(), schema, model);
    } else if (schema.getType().equals(Schema.Type.ENUM)) {
      return new AvroConverters.FieldEnumConverter(parent, schema, model);
    } else if (schema.getType().equals(Schema.Type.ARRAY)) {
      Class<?> datumClass = getDatumClass(schema, model);
      if (datumClass != null && datumClass.isArray()) {
        return new AvroArrayConverter(
            parent, type.asGroupType(), schema, model, datumClass);
      } else {
        return new AvroCollectionConverter(
            parent, type.asGroupType(), schema, model, datumClass);
      }
    } else if (schema.getType().equals(Schema.Type.MAP)) {
      return new MapConverter(parent, type.asGroupType(), schema, model);
    } else if (schema.getType().equals(Schema.Type.UNION)) {
      return new AvroUnionConverter(parent, type, schema, model);
    } else if (schema.getType().equals(Schema.Type.FIXED)) {
      return new AvroConverters.FieldFixedConverter(parent, schema, model);
    }
    throw new UnsupportedOperationException(String.format(
        "Cannot convert Avro type: %s to Parquet type: %s", schema, type));
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> getDatumClass(Schema schema, GenericData model) {
    if (model instanceof SpecificData) {
      // this works for reflect as well
      return ((SpecificData) model).getClass(schema);

    } else if (model.getClass() == GenericData.class) {
      return null;

    } else {
      // try to use reflection (for ThriftData and others)
      Class<? extends GenericData> modelClass = model.getClass();
      Method getClassMethod;
      try {
        getClassMethod = modelClass.getMethod("getClass", Schema.class);
      } catch (NoSuchMethodException e) {
        return null; // no getClass method
      }

      try {
        return (Class<T>) getClassMethod.invoke(schema);
      } catch (IllegalAccessException e) {
        return null;
      } catch (InvocationTargetException e) {
        return null;
      }
    }
  }

  protected void set(String name, int avroIndex, Object value) {
    model.setField(currentRecord, name, avroIndex, value);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  @SuppressWarnings("unchecked")
  public void start() {
    this.currentRecord = (T) model.newRecord(null, avroSchema);
  }

  @Override
  public void end() {
    fillInDefaults();
    if (parent != null) {
      parent.add(currentRecord);
    }
  }

  private void fillInDefaults() {
    for (Map.Entry<Schema.Field, Object> entry : recordDefaults.entrySet()) {
      Schema.Field f = entry.getKey();
      // replace following with model.deepCopy once AVRO-1455 is being used
      Object defaultValue = deepCopy(f.schema(), entry.getValue());
      set(f.name(), f.pos(), defaultValue);
    }
  }

  private Object deepCopy(Schema schema, Object value) {
    switch (schema.getType()) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return value;
      default:
        return model.deepCopy(schema, value);
    }
  }

  T getCurrentRecord() {
    return currentRecord;
  }

  /**
   * Converter for a list to a Java Collection.
   *
   * <pre>
   *   optional group the_list (LIST) { <-- this layer
   *     repeated group array {
   *       optional (type) element;
   *     }
   *   }
   * </pre>
   *
   * This class also implements LIST element backward-compatibility rules.
   */
  static final class AvroCollectionConverter extends GroupConverter {

    private final ParentValueContainer parent;
    private final Schema avroSchema;
    private final Converter converter;
    private Class<?> containerClass;
    private Collection<Object> container;

    public AvroCollectionConverter(ParentValueContainer parent, GroupType type,
                                   Schema avroSchema, GenericData model,
                                   Class<?> containerClass) {
      this.parent = parent;
      this.avroSchema = avroSchema;
      this.containerClass = containerClass;
      Schema elementSchema = this.avroSchema.getElementType();
      Type repeatedType = type.getType(0);
      // always determine whether the repeated type is the element type by
      // matching it against the element schema.
      if (isElementType(repeatedType, elementSchema)) {
        // the element type is the repeated type (and required)
        converter = newConverter(elementSchema, repeatedType, model, new ParentValueContainer() {
          @Override
          @SuppressWarnings("unchecked")
          public void add(Object value) {
            container.add(value);
          }
        });
      } else {
        // the element is wrapped in a synthetic group and may be optional
        converter = new ElementConverter(repeatedType.asGroupType(), elementSchema, model);
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converter;
    }

    @Override
    public void start() {
      container = newContainer();
    }

    @Override
    public void end() {
      parent.add(container);
    }

    @SuppressWarnings("unchecked")
    private Collection<Object> newContainer() {
      if (containerClass == null) {
        return new GenericData.Array<Object>(0, avroSchema);
      } else if (containerClass.isAssignableFrom(ArrayList.class)) {
        return new ArrayList<Object>();
      } else {
        // not need to use the data model to instantiate because it resolved
        // the class, which used the correct ClassLoader
        return (Collection<Object>) ReflectData.newInstance(containerClass, avroSchema);
      }
    }

    /**
     * Converter for list elements.
     *
     * <pre>
     *   optional group the_list (LIST) {
     *     repeated group array { <-- this layer
     *       optional (type) element;
     *     }
     *   }
     * </pre>
     */
    final class ElementConverter extends GroupConverter {
      private Object element;
      private final Converter elementConverter;

      public ElementConverter(GroupType repeatedType, Schema elementSchema, GenericData model) {
        Type elementType = repeatedType.getType(0);
        Schema nonNullElementSchema = AvroSchemaConverter.getNonNull(elementSchema);
        this.elementConverter = newConverter(nonNullElementSchema, elementType, model, new ParentValueContainer() {
          @Override
          @SuppressWarnings("unchecked")
          public void add(Object value) {
            ElementConverter.this.element = value;
          }
        });
      }

      @Override
      public Converter getConverter(int fieldIndex) {
        Preconditions.checkArgument(
            fieldIndex == 0, "Illegal field index: " + fieldIndex);
        return elementConverter;
      }

      @Override
      public void start() {
        element = null;
      }

      @Override
      public void end() {
        container.add(element);
      }
    }
  }

  /**
   * Converter for a list to a Java array.
   *
   * <pre>
   *   optional group the_list (LIST) { <-- this layer
   *     repeated group array {
   *       optional (type) element;
   *     }
   *   }
   * </pre>
   *
   * This class also implements LIST element backward-compatibility rules.
   */
  static final class AvroArrayConverter extends GroupConverter {

    private final ParentValueContainer parent;
    private final Schema avroSchema;
    private final Converter converter;
    private Class<?> elementClass;
    private Collection<?> container;

    public AvroArrayConverter(ParentValueContainer parent, GroupType type,
                              Schema avroSchema, GenericData model,
                              Class<?> arrayClass) {
      this.parent = parent;
      this.avroSchema = avroSchema;

      Preconditions.checkArgument(arrayClass.isArray(),
          "Cannot convert non-array: " + arrayClass.getName());
      this.elementClass = arrayClass.getComponentType();

      ParentValueContainer setter = createSetterAndContainer();
      Schema elementSchema = this.avroSchema.getElementType();
      Type repeatedType = type.getType(0);

      // always determine whether the repeated type is the element type by
      // matching it against the element schema.
      if (isElementType(repeatedType, elementSchema)) {
        // the element type is the repeated type (and required)
        converter = newConverter(elementSchema, repeatedType, model, setter);
      } else {
        // the element is wrapped in a synthetic group and may be optional
        converter = new PrimitiveElementConverter(
            repeatedType.asGroupType(), elementSchema, model, setter);
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converter;
    }

    @Override
    public void start() {
      // end creates a new copy of the array so the container is safe to reuse
      container.clear();
    }

    @Override
    public void end() {
      if (elementClass == boolean.class) {
        parent.add(((BooleanArrayList) container).toBooleanArray());
      } else if (elementClass == byte.class) {
        parent.add(((ByteArrayList) container).toByteArray());
      } else if (elementClass == char.class) {
        parent.add(((CharArrayList) container).toCharArray());
      } else if (elementClass == short.class) {
        parent.add(((ShortArrayList) container).toShortArray());
      } else if (elementClass == int.class) {
        parent.add(((IntArrayList) container).toIntArray());
      } else if (elementClass == long.class) {
        parent.add(((LongArrayList) container).toLongArray());
      } else if (elementClass == float.class) {
        parent.add(((FloatArrayList) container).toFloatArray());
      } else if (elementClass == double.class) {
        parent.add(((DoubleArrayList) container).toDoubleArray());
      } else {
        parent.add(((ArrayList) container).toArray());
      }
    }

    @SuppressWarnings("unchecked")
    private ParentValueContainer createSetterAndContainer() {
      if (elementClass == boolean.class) {
        final BooleanArrayList list = new BooleanArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addBoolean(boolean value) {
            list.add(value);
          }
        };
      } else if (elementClass == byte.class) {
        final ByteArrayList list = new ByteArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addByte(byte value) {
            list.add(value);
          }
        };
      } else if (elementClass == char.class) {
        final CharArrayList list = new CharArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addChar(char value) {
            list.add(value);
          }
        };
      } else if (elementClass == short.class) {
        final ShortArrayList list = new ShortArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addShort(short value) {
            list.add(value);
          }
        };
      } else if (elementClass == int.class) {
        final IntArrayList list = new IntArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addInt(int value) {
            list.add(value);
          }
        };
      } else if (elementClass == long.class) {
        final LongArrayList list = new LongArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addLong(long value) {
            list.add(value);
          }
        };
      } else if (elementClass == float.class) {
        final FloatArrayList list = new FloatArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addFloat(float value) {
            list.add(value);
          }
        };
      } else if (elementClass == double.class) {
        final DoubleArrayList list = new DoubleArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addDouble(double value) {
            list.add(value);
          }
        };
      } else {
        // this will end up as Object[]
        final List<Object> list = new ArrayList<Object>();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void add(Object value) {
            list.add(value);
          }
        };
      }

    }

    /**
     * Converter for primitive list elements.
     *
     * <pre>
     *   optional group the_list (LIST) {
     *     repeated group array { <-- this layer
     *       optional (type) element;
     *     }
     *   }
     * </pre>
     */
    final class PrimitiveElementConverter extends GroupConverter {
      private boolean isSet;
      private final Converter elementConverter;

      public PrimitiveElementConverter(GroupType repeatedType,
                                       Schema elementSchema, GenericData model,
                                       final ParentValueContainer setter) {
        Type elementType = repeatedType.getType(0);
        Preconditions.checkArgument(
            !elementClass.isPrimitive() || elementType.isRepetition(REQUIRED),
            "Cannot convert list of optional elements to primitive array");
        Schema nonNullElementSchema = AvroSchemaConverter.getNonNull(elementSchema);
        this.elementConverter = newConverter(
            nonNullElementSchema, elementType, model, new ParentValueContainer() {
              @Override
              public void add(Object value) {
                isSet = true;
                setter.add(value);
              }

              @Override
              public void addByte(byte value) {
                isSet = true;
                setter.addByte(value);
              }

              @Override
              public void addBoolean(boolean value) {
                isSet = true;
                setter.addBoolean(value);
              }

              @Override
              public void addChar(char value) {
                isSet = true;
                setter.addChar(value);
              }

              @Override
              public void addShort(short value) {
                isSet = true;
                setter.addShort(value);
              }

              @Override
              public void addInt(int value) {
                isSet = true;
                setter.addInt(value);
              }

              @Override
              public void addLong(long value) {
                isSet = true;
                setter.addLong(value);
              }

              @Override
              public void addFloat(float value) {
                isSet = true;
                setter.addFloat(value);
              }

              @Override
              public void addDouble(double value) {
                isSet = true;
                setter.addDouble(value);
              }
            });
      }

      @Override
      public Converter getConverter(int fieldIndex) {
        Preconditions.checkArgument(
            fieldIndex == 0, "Illegal field index: " + fieldIndex);
        return elementConverter;
      }

      @Override
      public void start() {
        isSet = false;
      }

      @Override
      public void end() {
        if (!isSet) {
          container.add(null);
        }
      }
    }
  }

  /**
   * Returns whether the given type is the element type of a list or is a
   * synthetic group with one field that is the element type. This is
   * determined by checking whether the type can be a synthetic group and by
   * checking whether a potential synthetic group matches the expected schema.
   * <p>
   * Unlike {@link AvroSchemaConverter#isElementType(Type, String)}, this
   * method never guesses because the expected schema is known.
   *
   * @param repeatedType a type that may be the element type
   * @param elementSchema the expected Schema for list elements
   * @return {@code true} if the repeatedType is the element schema
   */
  private static boolean isElementType(Type repeatedType, Schema elementSchema) {
    if (repeatedType.isPrimitive() ||
        repeatedType.asGroupType().getFieldCount() > 1) {
      // The repeated type must be the element type because it is an invalid
      // synthetic wrapper (must be a group with one field).
      return true;
    } else if (elementSchema != null &&
        elementSchema.getType() == Schema.Type.RECORD &&
        elementSchema.getFields().size() == 1 &&
        elementSchema.getFields().get(0).name().equals(
            repeatedType.asGroupType().getFieldName(0))) {
      // The repeated type must be the element type because it matches the
      // structure of the Avro element's schema.
      return true;
    }
    return false;
  }

  static final class AvroUnionConverter extends AvroConverters.AvroGroupConverter {
    private final Converter[] memberConverters;
    private Object memberValue = null;

    public AvroUnionConverter(ParentValueContainer parent, Type parquetSchema,
                              Schema avroSchema, GenericData model) {
      super(parent);
      GroupType parquetGroup = parquetSchema.asGroupType();
      this.memberConverters = new Converter[ parquetGroup.getFieldCount()];

      int parquetIndex = 0;
      for (int index = 0; index < avroSchema.getTypes().size(); index++) {
        Schema memberSchema = avroSchema.getTypes().get(index);
        if (!memberSchema.getType().equals(Schema.Type.NULL)) {
          Type memberType = parquetGroup.getType(parquetIndex);
          memberConverters[parquetIndex] = newConverter(memberSchema, memberType, model, new ParentValueContainer() {
            @Override
            public void add(Object value) {
              Preconditions.checkArgument(
                  AvroUnionConverter.this.memberValue == null,
                  "Union is resolving to more than one type");
              memberValue = value;
            }
          });
          parquetIndex++; // Note for nulls the parquetIndex id not increased
        }
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return memberConverters[fieldIndex];
    }

    @Override
    public void start() {
      memberValue = null;
    }

    @Override
    public void end() {
      parent.add(memberValue);
    }
  }

  static final class MapConverter<V> extends GroupConverter {

    private final ParentValueContainer parent;
    private final Converter keyValueConverter;
    private final Schema schema;
    private final Class<?> mapClass;
    private Map<String, V> map;

    public MapConverter(ParentValueContainer parent, GroupType mapType,
        Schema mapSchema, GenericData model) {
      this.parent = parent;
      GroupType repeatedKeyValueType = mapType.getType(0).asGroupType();
      this.keyValueConverter = new MapKeyValueConverter(
          repeatedKeyValueType, mapSchema, model);
      this.schema = mapSchema;
      this.mapClass = getDatumClass(mapSchema, model);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return keyValueConverter;
    }

    @Override
    public void start() {
      this.map = newMap();
    }

    @Override
    public void end() {
      parent.add(map);
    }

    @SuppressWarnings("unchecked")
    private Map<String, V> newMap() {
      if (mapClass == null || mapClass.isAssignableFrom(HashMap.class)) {
        return new HashMap<String, V>();
      } else {
        return (Map<String, V>) ReflectData.newInstance(mapClass, schema);
      }
    }

    final class MapKeyValueConverter extends GroupConverter {

      private String key;
      private V value;
      private final Converter keyConverter;
      private final Converter valueConverter;

      public MapKeyValueConverter(GroupType keyValueType, Schema mapSchema,
          GenericData model) {
        keyConverter = new PrimitiveConverter() {
          @Override
          final public void addBinary(Binary value) {
            key = value.toStringUsingUTF8();
          }
        };

        Type valueType = keyValueType.getType(1);
        Schema nonNullValueSchema = AvroSchemaConverter.getNonNull(mapSchema.getValueType());
        valueConverter = newConverter(nonNullValueSchema, valueType, model, new ParentValueContainer() {
          @Override
          @SuppressWarnings("unchecked")
          public void add(Object value) {
            MapKeyValueConverter.this.value = (V) value;
          }
        });
      }

      @Override
      public Converter getConverter(int fieldIndex) {
        if (fieldIndex == 0) {
          return keyConverter;
        } else if (fieldIndex == 1) {
          return valueConverter;
        }
        throw new IllegalArgumentException("only the key (0) and value (1) fields expected: " + fieldIndex);
      }

      @Override
      public void start() {
        key = null;
        value = null;
      }

      @Override
      public void end() {
        map.put(key, value);
      }
    }
  }
}
