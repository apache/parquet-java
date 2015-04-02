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
package parquet.filter2.recordlevel;

import parquet.column.Dictionary;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.PrimitiveType;

import static parquet.Preconditions.checkNotNull;

/**
 * see {@link FilteringRecordMaterializer}
 *
 * This pass-through proxy for a delegate {@link PrimitiveConverter} also
 * updates the {@link ValueInspector}s of a {@link IncrementallyUpdatedFilterPredicate}
 */
public class FilteringPrimitiveConverter extends PrimitiveConverter {
  private final PrimitiveConverter delegate;
  private final ValueInspector[] valueInspectors;
  private Binding binding;

  public FilteringPrimitiveConverter(PrimitiveConverter delegate, ValueInspector[] valueInspectors) {
    this.delegate = checkNotNull(delegate, "delegate");
    this.valueInspectors = checkNotNull(valueInspectors, "valueInspectors");
  }

  @Override
  public boolean hasDictionarySupport() {
    return true;
  }

  @Override
  public void setDictionary(Dictionary dictionary) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.setDictionary(dictionary);
    }

    if (delegate.hasDictionarySupport()) {
      delegate.setDictionary(dictionary);
    }

    binding = createBinding(dictionary);
  }


  @Override
  public void addValueFromDictionary(int dictionaryId) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.updateFromDictionary(dictionaryId);
    }
    binding.writeValue(dictionaryId);
  }

  @Override
  public void addBinary(Binary value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addBinary(value);
  }

  @Override
  public void addBoolean(boolean value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addBoolean(value);
  }

  @Override
  public void addDouble(double value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addDouble(value);
  }

  @Override
  public void addFloat(float value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addFloat(value);
  }

  @Override
  public void addInt(int value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addInt(value);
  }

  @Override
  public void addLong(long value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addLong(value);
  }

  abstract static class Binding {
    abstract void writeValue(int dictionaryId);
  }

  /**
   * Create binding based on dictionary or on type
   * @param dictionary Dictionary used in binding
   * @return Binding that sets value on delegate
   */
  private Binding createBinding(final Dictionary dictionary) {

    if (delegate.hasDictionarySupport()) {
      return new Binding() {
        @Override
        void writeValue(int dictionaryId) {
          delegate.addValueFromDictionary(dictionaryId);
        }
      };
    } else return dictionary.getPrimitiveTypeName().convert(new PrimitiveType.PrimitiveTypeNameConverter<Binding, RuntimeException>() {
      @Override
      public Binding convertFLOAT(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          void writeValue(int dictionaryId) {
            delegate.addFloat(dictionary.decodeToFloat(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertDOUBLE(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          void writeValue(int dictionaryId) {
            delegate.addDouble(dictionary.decodeToDouble(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertINT32(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          void writeValue(int dictionaryId) {
            delegate.addInt(dictionary.decodeToInt(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertINT64(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          void writeValue(int dictionaryId) {
            delegate.addLong(dictionary.decodeToLong(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertINT96(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          void writeValue(int dictionaryId) {
            delegate.addBinary(dictionary.decodeToBinary(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertFIXED_LEN_BYTE_ARRAY(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          void writeValue(int dictionaryId) {
            delegate.addBinary(dictionary.decodeToBinary(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertBOOLEAN(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          void writeValue(int dictionaryId) {
            delegate.addBoolean(dictionary.decodeToBoolean(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertBINARY(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          void writeValue(int dictionaryId) {
            delegate.addBinary(dictionary.decodeToBinary(dictionaryId));
          }
        };
      }
    });
  }
}
