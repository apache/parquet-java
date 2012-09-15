package redelm.thrift;

import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.FieldMetaData;

import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;

public class ThriftSchemaPrinter {

  public void print(Class<? extends TBase<?,?>> clazz) {
    TStructDescriptor struct = TStructDescriptor.getInstance(clazz);
    System.out.println("{");
    print("", struct);
    System.out.println("}");
  }

  private void print(String indent, TStructDescriptor struct) {
    List<Field> fields = struct.getFields();
    boolean first = true;
    for (Field field : fields) {
      if (first) {
        first = false;
      } else {
        System.out.println(",");
      }
      FieldMetaData tField = field.getFieldMetaData();
      String req;
      switch (tField.requirementType) {
      case TFieldRequirementType.REQUIRED:
        req = "REQUIRED";
        break;
      case TFieldRequirementType.OPTIONAL:
        req = "OPTIONAL";
        break;
      case TFieldRequirementType.DEFAULT:
        req = "DEFAULT";
        break;
      default:
        throw new IllegalArgumentException("unknown requirement type: " + tField.requirementType);
      }
      System.out.print(indent + req + " " + field.getName());
      if (field.isList()) {
        System.out.print(" REPEATED");
      } else if (field.isSet()) {
        System.out.print(" REPEATED");
      } else if (field.isStruct()) {
        System.out.println(indent + "{");
        print(indent + "  ", field.gettStructDescriptor());
        System.out.print(indent + "}");
      }
    }
    System.out.println();
  }
}
