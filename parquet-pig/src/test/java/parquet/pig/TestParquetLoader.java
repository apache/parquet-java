package parquet.pig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.pig.ExecType;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.newplan.logical.rules.ColumnPruneVisitor;
import org.junit.Before;
import org.junit.Test;

public class TestParquetLoader {

  private PigServer pigServer;
  private File logFile;

  @Before
  public void setUp() throws Exception{
    Logger logger = Logger.getLogger(ColumnPruneVisitor.class);
    logger.removeAllAppenders();
    logger.setLevel(Level.INFO);
    SimpleLayout layout = new SimpleLayout();
    logFile = File.createTempFile("log", "");
    FileAppender appender = new FileAppender(layout, logFile.toString(), false, false, 0);
    logger.addAppender(appender);

    pigServer = new PigServer(ExecType.LOCAL); 
  }
  
  @Test
  public void testSchema() throws Exception {
    String location = "target/out";
    ParquetLoader pLoader = new ParquetLoader("a:chararray, b:{t:(c:chararray, d:chararray)}, p:[(q:chararray, r:chararray)]");
    Job job = new Job();
    pLoader.getSchema(location, job);
    RequiredFieldList list = new RequiredFieldList();
    RequiredField field = new RequiredField("b", 0, 
        Arrays.asList(new RequiredField("t", 0, 
            Arrays.asList(new RequiredField("d", 1, null, DataType.CHARARRAY)), 
                DataType.TUPLE)), 
                DataType.BAG);
    list.add(field);
    pLoader.pushProjection(list);
    pLoader.setLocation(location, job);
    
    assertEquals("{b: {t: (d: chararray)}}",
        TupleReadSupport.getRequestedPigSchema(job.getConfiguration()).toString());
  }

  @Test
  public void testProjectionPushdown() throws Exception {
    String out = "target/out";
    int rows = 10;
    Data data = Storage.resetData(pigServer);
    Collection<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list.add(Storage.tuple(i, "a"+i));
    }
    data.set("in", "i:int, a:chararray", list );
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '"+out+"' using "+ParquetStorer.class.getName()+"();");
    pigServer.executeBatch();

    pigServer.registerQuery("C = LOAD '" + out + "' using " + ParquetLoader.class.getName()+"();");
    pigServer.registerQuery("D = foreach C generate i;");
    pigServer.registerQuery("Store D into 'out' using mock.Storage();");
    pigServer.executeBatch();

    assertTrue(checkLogFileMessage(new String[]{"Columns pruned for C: $1"}));
  }

  /**
   * sort subFields for LogMessages
   */
  public static List<String> sortSubFields(List<String> logMessages) {
    String regex = "\\[(.*)\\]";

    for (int i = 0; i < logMessages.size(); i++) {
      logMessages.set(i, sortString(regex, logMessages.get(i), ", "));
    }

    return logMessages;
  }

  /**
   * Find out the string which matches "regex" from "target", and sort the string with spacial
   * order. The string elements are split by "split".
   */
  public static String sortString(String regex, String target, String split) {
    Pattern p = Pattern.compile(regex);
    Matcher matcher = p.matcher(target);
    String original = null;
    String replaceString = new String();

    if (matcher.find()) {
      original = matcher.group(1);
      String[] out = original.split(split);
      Collections.sort(Arrays.asList(out));
      for (int j = 0; j < out.length; j++) {
        replaceString += (j > 0 ? ", " + out[j] : out[j]);
      }
      return target.replace(original, replaceString);
    }
    return target;
  }

  public boolean checkLogFileMessage(String[] messages) throws Exception {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(logFile));
      List<String> logMessages=new ArrayList<String>();
      String line;
      while ((line=reader.readLine())!=null)
      {
        logMessages.add(line);
      }
      if (logMessages.size() > 0) {
        logMessages = sortSubFields(logMessages);
      }

      // Check if all messages appear in the log
      for (int i=0;i<messages.length;i++)
      {
        boolean found = false;
        for (int j=0;j<logMessages.size();j++)
          if (logMessages.get(j).contains(messages[i])) {
            found = true;
            break;
          }
        if (!found)
          return false;
      }

      // Check no other log besides messages
      for (int i=0;i<logMessages.size();i++) {
        boolean found = false;
        for (int j=0;j<messages.length;j++) {
          if (logMessages.get(i).contains(messages[j])) {
            found = true;
            break;
          }
        }
        if (!found) {
          if (logMessages.get(i).contains("Columns pruned for")||
              logMessages.get(i).contains("Map key required for")) {
            return false;
          }
        }
      }
      return true;
    } catch (IOException e) {
      return false;
    } finally {
      reader.close();
    }
  }
}
