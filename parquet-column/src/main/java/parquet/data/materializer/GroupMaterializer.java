package parquet.data.materializer;

import static brennus.model.ExistingType.BOOLEAN;
import static brennus.model.ExistingType.INT;
import static brennus.model.ExistingType.OBJECT;
import static brennus.model.ExistingType.VOID;
import static brennus.model.ExistingType.existing;
import static brennus.model.Protection.PRIVATE;
import static brennus.model.Protection.PROTECTED;
import static brennus.model.Protection.PUBLIC;
import static parquet.schema.Type.Repetition.REPEATED;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import brennus.Builder;
import brennus.ClassBuilder;
import brennus.ConstructorBuilder;
import brennus.MethodBuilder;
import brennus.SwitchBuilder;
import brennus.asm.DynamicClassLoader;
import brennus.model.ExistingType;
import brennus.model.FutureType;
import parquet.data.Group;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;

public class GroupMaterializer extends RecordMaterializer<Group> {

  private static int id = 0;
  private static DynamicClassLoader cl = new DynamicClassLoader();

  private CompiledGroupConverter rootConverter;

  public GroupMaterializer(MessageType schema) {
    try {
      rootConverter = compileGroupConverter(0, schema, null, null);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private Iterable<Integer> getFields(boolean repeated, GroupType schema, Class<?> javaType) {
    List<Integer> result = new ArrayList<Integer>();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      final Type t = schema.getType(i);
      if (
            ((t.isRepetition(REPEATED) && repeated)
              || (!t.isRepetition(REPEATED) && ! repeated))
         && ((t.isPrimitive() && t.asPrimitiveType().getPrimitiveTypeName().javaType == javaType)
              || (!t.isPrimitive() && javaType == Group.class))
          ) {
          result.add(i);
      }
    }
    return result;
  }

  private Map<GroupType, Class<CompiledGroup>> groupTypes = new HashMap<GroupType, Class<CompiledGroup>>();

  private Class<CompiledGroup> generateGroupClass(GroupType schema) throws ReflectiveOperationException {
    if (groupTypes.containsKey(schema)) {
      return groupTypes.get(schema);
    }
    // generate group type
    ClassBuilder cb = new Builder().startClass(getClass().getName() + "$CompiledGroup" + (id++), existing(CompiledGroup.class));
    for (int i = 0; i < schema.getFieldCount(); i++) {
      Type t = schema.getType(i);
      Class<?> fieldType = getFieldType(t);
      cb = cb
          .field(PROTECTED, existing(fieldType), fieldName(i))
          .startMethod(PROTECTED, VOID, setterName(i)).param(existing(fieldType), "value")
            .exec().get("isSet").call("set").literal(i).endCall().endExec()
            .set(fieldName(i)).get("value").endSet()
          .endMethod();

      cb = cb
          .startMethod(PROTECTED, VOID, setDefined(i))
            .exec().get("isSet").call("set").literal(i).endCall().endExec()
          .endMethod();
    }

    Set<Class<?>> javaTypes = new HashSet<Class<?>>();
    for (PrimitiveTypeName ptm : PrimitiveTypeName.values()) {
      if (ptm.javaType != null) {
        javaTypes.add(ptm.javaType);
      }
    }
    javaTypes.add(Group.class);

    for (Class<?> jpt : javaTypes) {
      // repeated
      SwitchBuilder<MethodBuilder> switchBlock = cb.startMethod(PUBLIC, existing(jpt), "getRepeated" + setCase(jpt.getSimpleName()) + "At").param(INT, "fieldIndex").param(INT, "index")
          .switchOn().get("fieldIndex").switchBlock();
      for (int field : getFields(true, schema, jpt)) {
        switchBlock = switchBlock.caseBlock(field)
            .returnExp().get(fieldName(field)).getArrayValueAt().get("index").endGetArrayValue().endReturn()
            .endCase();
      }
      cb = endSwitchAndMethod(switchBlock);

      // non-repeated
      switchBlock = cb.startMethod(PUBLIC, existing(jpt), "get" + setCase(jpt.getSimpleName())).param(INT, "fieldIndex")
          .switchOn().get("fieldIndex").switchBlock();
      for (int field : getFields(false, schema, jpt)) {
        switchBlock = switchBlock.caseBlock(field)
            .returnExp().get(fieldName(field)).endReturn()
            .endCase();
      }
      cb = endSwitchAndMethod(switchBlock);
    }

    // repeated Object
    SwitchBuilder<MethodBuilder> switchBlock = cb.startMethod(PUBLIC, OBJECT, "getRepeatedValueAt").param(INT, "fieldIndex").param(INT, "index")
        .switchOn().get("fieldIndex").switchBlock();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      Type field = schema.getType(i);
      if (field.isRepetition(REPEATED)) {
        switchBlock = switchBlock.caseBlock(i)
          .returnExp().get(fieldName(i)).getArrayValueAt().get("index").endGetArrayValue().endReturn()
        .endCase();
      }
    }
    cb = endSwitchAndMethod(switchBlock);

    // non-repeated Object
    switchBlock = cb.startMethod(PUBLIC, OBJECT, "getValue").param(INT, "fieldIndex")
        .switchOn().get("fieldIndex").switchBlock();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      Type field = schema.getType(i);
      if (!field.isRepetition(REPEATED)) {
        switchBlock = switchBlock.caseBlock(i)
            .returnExp().get(fieldName(i)).endReturn()
            .endCase();
      }
    }
    cb = endSwitchAndMethod(switchBlock);

    switchBlock = cb.startMethod(PUBLIC, INT, "getRepetitionCount").param(INT, "fieldIndex")
        .switchOn().get("fieldIndex").switchBlock();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      Type t = schema.getType(i);
      if (t.isRepetition(REPEATED)) {
        switchBlock = switchBlock.caseBlock(i)
            .returnExp().get(fieldName(i)).arraySize().endReturn()
            .endCase();
      }
    }
    cb = endSwitchAndMethod(switchBlock);

    switchBlock = cb.startMethod(PUBLIC, BOOLEAN, "isDefined").param(INT, "fieldIndex")
        .switchOn().get("fieldIndex").switchBlock();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      Type t = schema.getType(i);
      if (!t.isRepetition(REPEATED)) {
//        if (t.isPrimitive() && t.asPrimitiveType().getPrimitiveTypeName() != BINARY) {
//          switchBlock = switchBlock.caseBlock(i)
//              .returnExp().literal(true).endReturn()
//              .endCase();
//        } else {
          switchBlock = switchBlock.caseBlock(i)
              .returnExp().get("isSet").call("get").literal(i).endCall().endReturn()
              .endCase();
//        }
      }
    }
    cb = endSwitchAndMethod(switchBlock);

    cb = cb
    .staticField(PUBLIC, existing(GroupType.class), "schema")
    .startMethod(PUBLIC, existing(GroupType.class), "getType")
      .returnExp().get("schema").endReturn()
    .endMethod();

    Class<CompiledGroup> result = load(cb.endClass());
    result.getDeclaredField("schema").set(null, schema);
    groupTypes.put(schema, result);
    return result;
  }

  private String setterName(int i) {
    return "setField_" + i;
  }

  private String setDefined(int i) {
    return "setFieldDefined_" + i;
  }

  private String fieldName(int field) {
    return "field_" + field;
  }

  private String setCase(String simpleName) {
    return simpleName.substring(0,1).toUpperCase() + simpleName.substring(1);
  }

  private Class<?> getFieldType(Type t) throws ReflectiveOperationException {
    Class<?> paramType = getParamType(t);
    Class<?> fieldType = t.isRepetition(REPEATED) ?
            Array.newInstance(paramType, 0).getClass()
          : paramType;
    return fieldType;
  }

  private Class<?> getParamType(Type t) throws ReflectiveOperationException {
    Class<?> fieldType;
    if (t.isPrimitive()) {
      PrimitiveType pt = t.asPrimitiveType();
      fieldType = pt.getPrimitiveTypeName().javaType;
    } else {
      GroupType gt = t.asGroupType();
      Class<?> subType = generateGroupClass(gt);
      fieldType = subType;
    }
    return fieldType;
  }

  @SuppressWarnings("unchecked")
  private <T> Class<T> load(FutureType futureType) throws ClassNotFoundException {
    return (Class<T>)cl.define(futureType);
  }

  private Class<CompiledGroupConverter> generateGroupConverterClass(int index, GroupType schema, Class<CompiledGroup> groupClass, Class<CompiledGroup> parent, Class<? extends CompiledGroupConverter> parentConverter) throws ReflectiveOperationException {
    ClassBuilder cb = new Builder().startClass(getClass().getName() + "$CompiledGroupConverter"+(id++), existing(CompiledGroupConverter.class));
    if (parent != null) {
      cb = cb
          .field(PUBLIC, existing(parentConverter), "parent");
    }
    cb = cb
        .field(PRIVATE, existing(groupClass), "currentRecord")
        .startMethod(PUBLIC, existing(Group.class), "getCurrentRecord")
          .returnExp().get("currentRecord").endReturn()
        .endMethod();

    for (int i = 0; i < schema.getFieldCount(); i++) {
      Type t = schema.getType(i);
      String fieldName = fieldName(i);
      cb = cb
          .field(PRIVATE, existing(GroupConverterField.getGroupConverterField(t)), fieldName)
          .startMethod(PUBLIC, VOID, "addToField_" + i).param(existing(getParamType(t)), "value")
            .exec().get("isSet").call("set").literal(i).endCall().endExec()
            .exec().get(fieldName).call("add").get("value").endCall().endExec()
          .endMethod();
    }

    ConstructorBuilder c = cb.startConstructor(PUBLIC).callSuperConstructorNoParam();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      Type t = schema.getType(i);
      String fieldName = fieldName(i);
      c = c.set(fieldName).newInstanceNoParam(existing(GroupConverterField.getGroupConverterField(t))).endSet();
    }
    cb = c.endConstructor();

    MethodBuilder m = cb
        .startMethod(PUBLIC, VOID, "end")
          .set("currentRecord").newInstanceNoParam(existing(groupClass)).endSet();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      Type t = schema.getType(i);
      String setterName = setterName(i);
      String fieldName = fieldName(i);
      if (t.isRepetition(REPEATED)) {
        m = m
            .exec().get("currentRecord")
              .call(setterName)
                .get(fieldName)
                  .call("get")
                    .newArrayOfSize(existing(getParamType(t)))
                      .get(fieldName).callNoParam("size")
                    .endNewArray()
                  .endCall()
              .endCall()
            .endExec();
      } else {
        m = m.ifExp().get("isSet").call("get").literal(i).endCall().thenBlock()
              .exec().get("currentRecord")
                .call(setterName)
                  .get(fieldName).callNoParam("get")
                .endCall()
              .endExec()
            .endIf();
      }
    }
    m = m.exec().get("isSet").callNoParam("clear").endExec();
    if (parent != null) {
      m = m.exec().get("parent").call("addToField_" + index).get("currentRecord").endCall().endExec();
    }
    cb = m.endMethod();
    return load(cb.endClass());
  }

  private ClassBuilder endSwitchAndMethod(
      SwitchBuilder<MethodBuilder> switchBlock) {
    return switchBlock
        .defaultCase()
          .throwExp().callOnThis("error").get("fieldIndex").endCall().endThrow()
          .breakCase()
        .endSwitch()
      .endMethod();
  }

  private Converter compileConverter(int index, Type type, Class<CompiledGroup> parent, CompiledGroupConverter inst) throws ReflectiveOperationException {
    Converter c;
    if (type.isPrimitive()) {
      c = compilePrimitiveConverter(index, type.asPrimitiveType(), parent, inst);
    } else {
      c = compileGroupConverter(index, type.asGroupType(), parent, inst.getClass());
    }
    c.getClass().getDeclaredField("parent").set(c, inst);
    return c;
  }

  private Converter compilePrimitiveConverter(int index, PrimitiveType type, Class<CompiledGroup> parent, CompiledGroupConverter inst) throws ReflectiveOperationException {
    return generatePrimitiveConverter(index, type, inst.getClass(), parent).newInstance();
  }

  private Class<? extends CompiledPrimitiveConverter> generatePrimitiveConverter(int index, PrimitiveType type, Class<? extends CompiledGroupConverter> parentConverter, Class<? extends CompiledGroup> parent) throws ClassNotFoundException {
    String addMethod = "add" + type.getPrimitiveTypeName().getMethod.substring(3);
    ExistingType paramType = existing(type.getPrimitiveTypeName().javaType);
    ClassBuilder cb = new Builder()
      .startClass(getClass().getName() + "$CompiledPrimitiveConverter" + (id++), existing(CompiledPrimitiveConverter.class))
        .field(PUBLIC, existing(parentConverter), "parent")
        .startMethod(PUBLIC, VOID, addMethod).param(paramType, "value")
          .exec().get("parent").call("addToField_" + index).get("value").endCall().endExec()
        .endMethod();
    return load(cb.endClass());
  }

  private CompiledGroupConverter compileGroupConverter(int index, GroupType schema, Class<CompiledGroup> parent, Class<? extends CompiledGroupConverter> parentConverter) throws ReflectiveOperationException {
    Class<CompiledGroup> groupClass = generateGroupClass(schema);
    Class<CompiledGroupConverter> clazz = generateGroupConverterClass(index, schema, groupClass, parent, parentConverter);
    CompiledGroupConverter inst = clazz.newInstance();
    Converter[] children = new Converter[schema.getFieldCount()];
    for (int i = 0; i < children.length; i++) {
      children[i] = compileConverter(i, schema.getType(i), groupClass, inst);
    }
    inst.converters = children;
    return inst;
  }

  @Override
  public Group getCurrentRecord() {
    return rootConverter.getCurrentRecord();
  }

  @Override
  public GroupConverter getRootConverter() {
    return rootConverter;
  }

}
