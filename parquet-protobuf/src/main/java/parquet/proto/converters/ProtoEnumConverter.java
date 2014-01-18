/**
 * Copyright 2013 Lukas Nalezenec.
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

package parquet.proto.converters;

import com.google.protobuf.Descriptors;
import parquet.column.Dictionary;
import parquet.io.InvalidRecordException;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class ProtoEnumConverter extends PrimitiveConverter {

  private final Descriptors.FieldDescriptor fieldType;
  private final Map<Binary, Descriptors.EnumValueDescriptor> enumLookup;
  private Descriptors.EnumValueDescriptor[] dict;
  private final ParentValueContainer parent;

  public ProtoEnumConverter(ParentValueContainer parent, Descriptors.FieldDescriptor fieldType) {
    this.parent = parent;
    this.fieldType = fieldType;
    this.enumLookup = makeLookupStructure(fieldType);
  }

  /**
   * Fills lookup structure for translating between parquet enum values and Protocol buffer enum values.
   * */
  private Map<Binary, Descriptors.EnumValueDescriptor> makeLookupStructure(Descriptors.FieldDescriptor enumFieldType) {
    Descriptors.EnumDescriptor enumType = enumFieldType.getEnumType();
    Map<Binary, Descriptors.EnumValueDescriptor> lookupStructure = new HashMap<Binary, Descriptors.EnumValueDescriptor>();

    List<Descriptors.EnumValueDescriptor> enumValues = enumType.getValues();

    for (Descriptors.EnumValueDescriptor value : enumValues) {
      String name = value.getName();
      lookupStructure.put(Binary.fromString(name), enumType.findValueByName(name));
    }

    return lookupStructure;
  }

  /**
   * Translates given parquet enum value to protocol buffer enum value.
   * @throws InvalidRecordException is there is no corresponding value.
   * */
  private Descriptors.EnumValueDescriptor translateEnumValue(Binary binaryValue) {
    Descriptors.EnumValueDescriptor protoValue = enumLookup.get(binaryValue);

    if (protoValue == null) {
      Set<Binary> knownValues = enumLookup.keySet();
      String msg = "Illegal enum value \"" + binaryValue + "\""
              + " in protocol buffer \"" + fieldType.getFullName() + "\""
              + " legal values are: \"" + knownValues + "\"";
      throw new InvalidRecordException(msg);
    }
    return protoValue;
  }

  @Override
  final public void addBinary(Binary binaryValue) {
    Descriptors.EnumValueDescriptor protoValue = translateEnumValue(binaryValue);
    parent.add(protoValue);
  }

  @Override
  public void addValueFromDictionary(int dictionaryId) {
    parent.add(dict[dictionaryId]);
  }

  @Override
  public boolean hasDictionarySupport() {
    return true;
  }

  @Override
  public void setDictionary(Dictionary dictionary) {
    dict = new  Descriptors.EnumValueDescriptor[dictionary.getMaxId() + 1];
    for (int i = 0; i <= dictionary.getMaxId(); i++) {
      Binary binaryValue = dictionary.decodeToBinary(i);
      dict[i] = translateEnumValue(binaryValue);
    }
  }

}
