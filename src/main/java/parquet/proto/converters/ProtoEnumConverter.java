package parquet.proto.converters;

import com.google.protobuf.Descriptors;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ProtoEnumConverter extends PrimitiveConverter {

  private final Descriptors.FieldDescriptor fieldType;
  private final Map<Binary, Descriptors.EnumValueDescriptor> enumLookup;
  private final ParentValueContainer parent;

  public ProtoEnumConverter(ParentValueContainer parent, Descriptors.FieldDescriptor fieldType) {
    this.parent = parent;
    this.fieldType = fieldType;
    this.enumLookup = makeLookupStructure(fieldType);
  }

  Map<Binary, Descriptors.EnumValueDescriptor> makeLookupStructure(Descriptors.FieldDescriptor enumFieldType) {
    Descriptors.EnumDescriptor enumType = enumFieldType.getEnumType();
    Map<Binary, Descriptors.EnumValueDescriptor> lookupStructure = new HashMap<Binary, Descriptors.EnumValueDescriptor>();

    List<Descriptors.EnumValueDescriptor> enumValues = enumType.getValues();

    for (Descriptors.EnumValueDescriptor value : enumValues) {
      String name = value.getName();
      lookupStructure.put(Binary.fromString(name), enumType.findValueByName(name));
    }

    return lookupStructure;
  }

  @Override
  final public void addBinary(Binary value) {
    Descriptors.EnumValueDescriptor protoValue = enumLookup.get(value);

    if (protoValue == null) {
      throw new RuntimeException("Illegal enum value " + value + " in protoBuffer " + fieldType.getFullName());
    }

    parent.add(protoValue);
  }

}
