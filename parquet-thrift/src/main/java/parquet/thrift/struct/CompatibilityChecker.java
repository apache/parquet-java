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
       }
       return true;
     }

}


