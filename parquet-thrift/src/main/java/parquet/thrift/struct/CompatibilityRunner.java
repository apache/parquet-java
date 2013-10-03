package parquet.thrift.struct;


import org.apache.thrift.TBase;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
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
      //java CompatibilityRunner generate-json tfe_request com.twitter.logs.TfeRequestLog old_json/
      generateJson(arguments);
    }
    if (operator.equals("compare")){
      checkCompatible(arguments);
    }
  }

  private static void checkCompatible(LinkedList<String> arguments) throws ClassNotFoundException, IOException {
    String catName=arguments.pollFirst();
    String className=arguments.pollFirst();
    String storedPath=arguments.pollFirst();
    File dirForOldJson=new File(storedPath);
    String oldJsonName=catName+".json";

    ThriftType.StructType newStruct = new ThriftSchemaConverter().toStructType((Class<? extends TBase<?, ?>>) Class.forName(className));

    ObjectMapper mapper = new ObjectMapper();
    File jsonFile = new File(dirForOldJson, oldJsonName);

    if (!jsonFile.exists()){
      System.out.println(jsonFile+"does not exist, nothing to check against to");
      return;
    }

    ThriftType.StructType oldStruct= mapper.readValue(jsonFile,ThriftType.StructType.class);
    CompatibilityReport report= new CompatibilityChecker().checkCompatibility(oldStruct,newStruct);
    if(!report.isCompatible){
      System.err.println("schema not compatible");
      System.err.println(report.getMessages());
      System.exit(1);
    }
    System.out.println("[success] schema is compatible");
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
