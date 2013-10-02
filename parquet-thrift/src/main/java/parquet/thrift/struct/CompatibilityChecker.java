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
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void visit(ThriftType.SetType setType) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void visit(ThriftType.ListType listType) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public void fail() throws RuntimeException{
    throw new RuntimeException("fail!!!");
  }

  @Override
  public void visit(ThriftType.StructType newStruct) {
    ThriftType.StructType currentOldType= ((ThriftType.StructType)oldType);

    for(ThriftField oldField: currentOldType.getChildren()){
      short fieldId = oldField.getFieldId();
      ThriftField newField= newStruct.getChildById(fieldId);
      if (newField==null)
        fail();
      if (!newField.getType().equals(oldField.getType()))
        fail();
      if(!newField.getName().equals(oldField.getName()))
        fail();
      oldType=oldField.getType();
      newField.getType().accept(this);
      //TODO: fail with message
      //TODO: check requirement
      //TODO: recursivly, visitor pattern?
    }

    //restore
    oldType=currentOldType;
  }

  @Override
  public void visit(ThriftType.EnumType enumType) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void visit(ThriftType.BoolType boolType) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void visit(ThriftType.ByteType byteType) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void visit(ThriftType.DoubleType doubleType) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void visit(ThriftType.I16Type i16Type) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void visit(ThriftType.I32Type i32Type) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void visit(ThriftType.I64Type i64Type) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void visit(ThriftType.StringType stringType) {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}


