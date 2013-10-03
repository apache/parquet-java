package parquet.thrift.struct;


import java.util.ArrayList;
import java.util.List;

public class CompatibilityChecker {
  public boolean areCompatible(ThriftType.StructType oldStruct, ThriftType.StructType newStruct) {
    return checkCompatibility(oldStruct, newStruct).isCompatible();
  }

  public CompatibilityReport checkCompatibility(ThriftType.StructType oldStruct, ThriftType.StructType newStruct) {
    CompatibleCheckerVisitor visitor = new CompatibleCheckerVisitor(oldStruct);
    newStruct.accept(visitor);
    return visitor.getReport();
  }

}

class CompatibilityReport {
  boolean isCompatible = true;
  List<String> messages = new ArrayList<String>();

  public boolean isCompatible() {
    return isCompatible;
  }

  public void fail(String message) {
    messages.add(message);
    isCompatible = false;
  }
}

class CompatibleCheckerVisitor implements ThriftType.TypeVisitor {
  ThriftType oldType;
  CompatibilityReport report = new CompatibilityReport();

  CompatibleCheckerVisitor(ThriftType.StructType oldType) {
    this.oldType = oldType;
  }

  public CompatibilityReport getReport() {
    return report;
  }

  @Override
  public void visit(ThriftType.MapType mapType) {
    ThriftType.MapType currentOldType = ((ThriftType.MapType) oldType);
    ThriftField oldKeyField = currentOldType.getKey();
    ThriftField newKeyField = mapType.getKey();

    ThriftField newValueField = mapType.getValue();
    ThriftField oldValueField = currentOldType.getValue();

    checkField(oldKeyField, newKeyField);
    checkField(oldValueField, newValueField);

    //restore TODO: is this necessary?
    oldType = currentOldType;
  }

  @Override
  public void visit(ThriftType.SetType setType) {
    ThriftType.SetType currentOldType = ((ThriftType.SetType) oldType);
    ThriftField oldField = currentOldType.getValues();
    ThriftField newField = setType.getValues();
    checkField(oldField, newField);
    oldType = currentOldType;
  }

  @Override
  public void visit(ThriftType.ListType listType) {
    ThriftType.ListType currentOldType = ((ThriftType.ListType) oldType);
    ThriftField oldField = currentOldType.getValues();
    ThriftField newField = listType.getValues();
    checkField(oldField, newField);
    oldType = currentOldType;
  }

  public void fail(String message) {
    report.fail(message);
  }

  private void checkField(ThriftField oldField, ThriftField newField) {

    if (!newField.getType().getType().equals(oldField.getType().getType())) {//TODO: check equals method for ThriftTypeID
      fail("type is not compatible " + oldField.getType().getType() + " vs " + newField.getType().getType());
      return;
    }


    if (!newField.getName().equals(oldField.getName())) {
      fail("field names are different " + oldField.getName() + " vs " + newField.getName());
      return;
    }

    if (firstIsMoreRestirctive(newField.getRequirement(), oldField.getRequirement())){
      fail("new field is more restrictive "+ newField.getName());
      return;
    }

      oldType = oldField.getType();
    newField.getType().accept(this);
  }

  private boolean firstIsMoreRestirctive(ThriftField.Requirement firstReq, ThriftField.Requirement secReq) {

    if (firstReq == ThriftField.Requirement.REQUIRED && secReq == ThriftField.Requirement.OPTIONAL) {
      return true;
    } else {
      return false;
    }

  }

  @Override
  public void visit(ThriftType.StructType newStruct) {
    ThriftType.StructType currentOldType = ((ThriftType.StructType) oldType);

    for (ThriftField oldField : currentOldType.getChildren()) {
      short fieldId = oldField.getFieldId();
      ThriftField newField = null;
      try {
        newField = newStruct.getChildById(fieldId);
      } catch (ArrayIndexOutOfBoundsException e) {
        fail("can not find index in new Struct: " + fieldId);
        return;
      }
      checkField(oldField, newField);
      //TODO: fail with message
      //TODO: check requirement
      //TODO: recursivly, visitor pattern?
    }

    //restore
    oldType = currentOldType;
  }

  @Override
  public void visit(ThriftType.EnumType enumType) {
    return;
  }

  @Override
  public void visit(ThriftType.BoolType boolType) {
    return;
  }

  @Override
  public void visit(ThriftType.ByteType byteType) {
    return;
  }

  @Override
  public void visit(ThriftType.DoubleType doubleType) {
    return;
  }

  @Override
  public void visit(ThriftType.I16Type i16Type) {
    return;
  }

  @Override
  public void visit(ThriftType.I32Type i32Type) {
    return;
  }

  @Override
  public void visit(ThriftType.I64Type i64Type) {
    return;
  }

  @Override
  public void visit(ThriftType.StringType stringType) {
    return;
  }
}


