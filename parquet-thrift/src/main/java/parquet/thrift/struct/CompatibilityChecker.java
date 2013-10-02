package parquet.thrift.struct;

public class CompatibilityChecker {
     public boolean areCompatible(ThriftType.StructType oldStruct, ThriftType.StructType newStruct){

       for(ThriftField oldField:oldStruct.getChildren()){
         short fieldId = oldField.getFieldId();
         ThriftField newField= newStruct.getChildById(fieldId);
         if (newField==null)
           return false;
         if (!newField.getType().equals(oldField.getType()))
           return false;
         if(!newField.getName().equals(oldField.getName()))
           return false;
         //TODO: check requirement
         //TODO: recursivly, visitor pattern?
       }
       return true;
     }

}

class CompatibleCheckerVisitor implements ThriftType.TypeVisitor{
  ThriftType oldType;

  CompatibleCheckerVisitor(ThriftType.StructType oldType) {
    this.oldType = oldType;
  }


  @Override
  public void visit(ThriftType.MapType mapType) {
    ThriftType.MapType currentOldType= ((ThriftType.MapType)oldType);
    ThriftField oldKeyField=currentOldType.getKey();
    ThriftField newKeyField=mapType.getKey();

    ThriftField newValueField=mapType.getValue();
    ThriftField oldValueField=currentOldType.getValue();

    checkField(oldKeyField,newKeyField);
    checkField(oldValueField,newValueField);

    //restore TODO: is this necessary?
    oldType=currentOldType;
  }

  @Override
  public void visit(ThriftType.SetType setType) {
    ThriftType.SetType currentOldType= ((ThriftType.SetType)oldType);
    ThriftField oldField=currentOldType.getValues();
    ThriftField newField=setType.getValues();
    checkField(oldField,newField);
    oldType=currentOldType;
  }

  @Override
  public void visit(ThriftType.ListType listType) {
    ThriftType.ListType currentOldType= ((ThriftType.ListType)oldType);
    ThriftField oldField=currentOldType.getValues();
    ThriftField newField=listType.getValues();
    checkField(oldField,newField);
    oldType=currentOldType;
  }

  public void fail() throws RuntimeException{
    throw new RuntimeException("fail!!!");
  }

  private void checkField(ThriftField oldField, ThriftField newField){
    if (newField==null)
      fail();
    if (!newField.getType().getType().equals(oldField.getType().getType()))//TODO: check equals method for ThriftTypeID
      fail();
    if(!newField.getName().equals(oldField.getName()))
      fail();
    oldType=oldField.getType();
    newField.getType().accept(this);
  }

  @Override
  public void visit(ThriftType.StructType newStruct) {
    ThriftType.StructType currentOldType= ((ThriftType.StructType)oldType);

    for(ThriftField oldField: currentOldType.getChildren()){
      short fieldId = oldField.getFieldId();
      ThriftField newField= newStruct.getChildById(fieldId);
      checkField(oldField,newField);
      //TODO: fail with message
      //TODO: check requirement
      //TODO: recursivly, visitor pattern?
    }

    //restore
    oldType=currentOldType;
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


