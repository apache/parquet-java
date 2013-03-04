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
package parquet.thrift.struct;

import static parquet.thrift.struct.ThriftTypeID.BOOL;
import static parquet.thrift.struct.ThriftTypeID.BYTE;
import static parquet.thrift.struct.ThriftTypeID.DOUBLE;
import static parquet.thrift.struct.ThriftTypeID.ENUM;
import static parquet.thrift.struct.ThriftTypeID.I16;
import static parquet.thrift.struct.ThriftTypeID.I32;
import static parquet.thrift.struct.ThriftTypeID.I64;
import static parquet.thrift.struct.ThriftTypeID.LIST;
import static parquet.thrift.struct.ThriftTypeID.MAP;
import static parquet.thrift.struct.ThriftTypeID.SET;
import static parquet.thrift.struct.ThriftTypeID.STRING;
import static parquet.thrift.struct.ThriftTypeID.STRUCT;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonTypeInfo.As;
import org.codehaus.jackson.annotate.JsonTypeInfo.Id;
import org.codehaus.jackson.annotate.JsonTypeName;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.codehaus.jackson.map.annotate.JacksonStdImpl;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonTypeIdResolver;
import org.codehaus.jackson.map.jsontype.TypeIdResolver;
import org.codehaus.jackson.map.type.SimpleType;
import org.codehaus.jackson.type.JavaType;

@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "id")
@JsonSubTypes({
    @JsonSubTypes.Type(value=ThriftType.BoolType.class, name="BOOL"),
    @JsonSubTypes.Type(value=ThriftType.ByteType.class, name="BYTE"),
    @JsonSubTypes.Type(value=ThriftType.DoubleType.class, name="DOUBLE"),
    @JsonSubTypes.Type(value=ThriftType.EnumType.class, name="ENUM"),
    @JsonSubTypes.Type(value=ThriftType.I16Type.class, name="I16"),
    @JsonSubTypes.Type(value=ThriftType.I32Type.class, name="I32"),
    @JsonSubTypes.Type(value=ThriftType.I64Type.class, name="I64"),
    @JsonSubTypes.Type(value=ThriftType.ListType.class, name="LIST"),
    @JsonSubTypes.Type(value=ThriftType.MapType.class, name="MAP"),
    @JsonSubTypes.Type(value=ThriftType.SetType.class, name="SET"),
    @JsonSubTypes.Type(value=ThriftType.StringType.class, name="STRING"),
    @JsonSubTypes.Type(value=ThriftType.StructType.class, name="STRUCT")
})
public abstract class ThriftType {

  public static ThriftType fromJSON(String json) {
    return JSON.fromJSON(json, ThriftType.class);
  }

  public String toJSON() {
    return JSON.toJSON(this);
  }

  @Override
  public String toString() {
    return toJSON();
  }

  public interface TypeVisitor {

    void visit(MapType mapType);

    void visit(SetType setType);

    void visit(ListType listType);

    void visit(StructType structType);

    void visit(EnumType enumType);

    void visit(BoolType boolType);

    void visit(ByteType byteType);

    void visit(DoubleType doubleType);

    void visit(I16Type i16Type);

    void visit(I32Type i32Type);

    void visit(I64Type i64Type);

    void visit(StringType stringType);

  }

  public static abstract class ComplexTypeVisitor implements TypeVisitor {

    @Override
    final public void visit(EnumType enumType) {
      throw new IllegalArgumentException("Expected complex type");
    }

    @Override
    final public void visit(BoolType boolType) {
      throw new IllegalArgumentException("Expected complex type");
    }

    @Override
    final public void visit(ByteType byteType) {
      throw new IllegalArgumentException("Expected complex type");
    }

    @Override
    final public void visit(DoubleType doubleType) {
      throw new IllegalArgumentException("Expected complex type");
    }

    @Override
    final public void visit(I16Type i16Type) {
      throw new IllegalArgumentException("Expected complex type");
    }

    @Override
    final public void visit(I32Type i32Type) {
      throw new IllegalArgumentException("Expected complex type");
    }

    @Override
    final public void visit(I64Type i64Type) {
      throw new IllegalArgumentException("Expected complex type");
    }

    @Override
    final public void visit(StringType stringType) {
      throw new IllegalArgumentException("Expected complex type");
    }

  }

  public static class StructType extends ThriftType {
    private final List<ThriftField> children;

    @JsonCreator
    public StructType(@JsonProperty("children") List<ThriftField> children) {
      super(STRUCT);
      this.children = children;
    }

    public List<ThriftField> getChildren() {
      return children;
    }

    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }

  }

  public static class MapType extends ThriftType {
    private final ThriftField key;
    private final ThriftField value;

    @JsonCreator
    public MapType(@JsonProperty("key") ThriftField key, @JsonProperty("value") ThriftField value) {
      super(MAP);
      this.key = key;
      this.value = value;
    }

    public ThriftField getKey() {
      return key;
    }

    public ThriftField getValue() {
      return value;
    }

    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }

  }

  public static class SetType extends ThriftType {
    private final ThriftField values;

    @JsonCreator
    public SetType(@JsonProperty("values") ThriftField values) {
      super(SET);
      this.values = values;
    }

    public ThriftField getValues() {
      return values;
    }

    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }

  }

  public static class ListType extends ThriftType {
    private final ThriftField values;

    @JsonCreator
    public ListType(@JsonProperty("values") ThriftField values) {
      super(LIST);
      this.values = values;
    }

    public ThriftField getValues() {
      return values;
    }

    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }

  }

  public static class EnumValue {
    private final int id;
    private final String name;

    @JsonCreator
    public EnumValue(@JsonProperty("id") int id, @JsonProperty("name") String name) {
      super();
      this.id = id;
      this.name = name;
    }
    public int getId() {
      return id;
    }
    public String getName() {
      return name;
    }
  }

  public static class EnumType extends ThriftType {
    private final List<EnumValue> values;

    @JsonCreator
    public EnumType(@JsonProperty("values") List<EnumValue> values) {
      super(ENUM);
      this.values = values;
    }

    public List<EnumValue> getValues() {
      return values;
    }

    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }
  }

  public static class BoolType extends ThriftType {

    @JsonCreator
    public BoolType() {
      super(BOOL);
    }
    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }
  }

  public static class ByteType extends ThriftType {

    @JsonCreator
    public ByteType() {
      super(BYTE);
    }
    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }
  }

  public static class DoubleType extends ThriftType {

    @JsonCreator
    public DoubleType() {
      super(DOUBLE);
    }
    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }
  }

  public static class I16Type extends ThriftType {

    @JsonCreator
    public I16Type() {
      super(I16);
    }
    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }
  }

  public static class I32Type extends ThriftType {

    @JsonCreator
    public I32Type() {
      super(I32);
    }
    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }
  }

  public static class I64Type extends ThriftType {

    @JsonCreator
    public I64Type() {
      super(I64);
    }
    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }
  }

  public static class StringType extends ThriftType {

    @JsonCreator
    public StringType() {
      super(STRING);
    }
    @Override
    public void accept(TypeVisitor visitor) {
      visitor.visit(this);
    }
  }

  private final ThriftTypeID type;

  private ThriftType(ThriftTypeID type) {
    super();
    this.type = type;
  }

  public abstract void accept(TypeVisitor visitor);

  @JsonIgnore
  public ThriftTypeID getType() {
    return this.type;
  }

}
