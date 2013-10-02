package parquet.thrift.struct;


import org.apache.thrift.TBase;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.ObjectMapper;
import parquet.thrift.ThriftSchemaConverter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class CompatibilityRunner {
  public static void main(String[] args) throws Exception{
    LinkedList<String> arguments = new LinkedList<String>(Arrays.asList(args));
    String operator = arguments.pollFirst();
    if (operator.equals("generate-json")){
      generateJson(arguments);
    }
  }

  private static void generateJson(LinkedList<String> arguments) throws ClassNotFoundException, IOException {
    String catName=arguments.pollFirst();
    String className=arguments.pollFirst();
    String storedPath=arguments.pollFirst();
    File storeDir=new File(storedPath);
    ThriftType.StructType structType = new ThriftSchemaConverter().toStructType((Class<? extends TBase<?, ?>>) Class.forName(className));
    ObjectMapper mapper = new ObjectMapper();

    String fileName=catName+".json";
    mapper.writeValue(new File(storeDir,fileName),structType);
  }
}
