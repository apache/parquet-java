package parquet.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import parquet.proto.ProtoParquetWriter;
import parquet.proto.test.ProtoName;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class TestParquetProtoScheme {
  final String txtInputPath = "src/test/resources/names.txt";
  final String parquetInputPath = "target/test/ParquetProtoScheme/names-parquet-in";
  final String parquetOutputPath = "target/test/ParquetProtoScheme/names-parquet-out";
  final String txtOutputPath = "target/test/ParquetProtoScheme/names-txt-out";

  @Test
  public void testWrite() throws Exception {
    Path path = new Path(parquetOutputPath);
    final FileSystem fs = path.getFileSystem(new Configuration());
    if (fs.exists(path)) fs.delete(path, true);

    Scheme sourceScheme = new TextLine( new Fields( "first", "last" ) );
    Tap source = new Hfs(sourceScheme, txtInputPath);

    Scheme sinkScheme = new ParquetProtoScheme<ProtoName.Name>(ProtoName.Name.class);
    Tap sink = new Hfs(sinkScheme, parquetOutputPath);

    Pipe assembly = new Pipe( "namecp" );
    assembly = new Each(assembly, new PackProtoFunction());
    Flow flow  = new HadoopFlowConnector().connect("namecp", source, sink, assembly);

    flow.complete();
  }

  @Test
  public void testRead() throws Exception {
    doRead(new ParquetProtoScheme<ProtoName.Name>(ProtoName.Name.class));
  }

  @Test
  public void testReadWithoutClass() throws Exception {
    doRead(new ParquetProtoScheme());
  }

  private void doRead(Scheme sourceScheme) throws Exception {
    createFileForRead();

    Path path = new Path(txtOutputPath);
    final FileSystem fs = path.getFileSystem(new Configuration());
    if (fs.exists(path)) fs.delete(path, true);

    Tap source = new Hfs(sourceScheme, parquetInputPath);

    Scheme sinkScheme = new TextLine(new Fields("first", "last"));
    Tap sink = new Hfs(sinkScheme, txtOutputPath);

    Pipe assembly = new Pipe( "namecp" );
    assembly = new Each(assembly, new UnpackProtoFunction());
    Flow flow  = new HadoopFlowConnector().connect("namecp", source, sink, assembly);

    flow.complete();
    String result = FileUtils.readFileToString(new File(txtOutputPath+"/part-00000"));
    assertEquals("Alice\tPractice\nBob\tHope\nCharlie\tHorse\n", result);
  }


  private void createFileForRead() throws Exception {
    final Path fileToCreate = new Path(parquetInputPath+"/names.parquet");

    final Configuration conf = new Configuration();
    final FileSystem fs = fileToCreate.getFileSystem(conf);
    if (fs.exists(fileToCreate)) fs.delete(fileToCreate, true);

    ProtoParquetWriter<ProtoName.Name> w = new ProtoParquetWriter<ProtoName.Name>(fileToCreate, ProtoName.Name.class);

    ProtoName.Name n1 = ProtoName.Name.newBuilder().setFirstName("Alice").setLastName("Practice").build();
    ProtoName.Name n2 = ProtoName.Name.newBuilder().setFirstName("Bob").setLastName("Hope").build();
    ProtoName.Name n3 = ProtoName.Name.newBuilder().setFirstName("Charlie").setLastName("Horse").build();

    w.write(n1);
    w.write(n2);
    w.write(n3);
    w.close();
  }

  private static class PackProtoFunction extends BaseOperation implements Function {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      TupleEntry arguments = functionCall.getArguments();
      Tuple result = new Tuple();

      result.add(ProtoName.Name.newBuilder().setFirstName(arguments.getString(0)).setLastName(arguments.getString(1)).build());
      functionCall.getOutputCollector().add(result);
    }
  }

  private static class UnpackProtoFunction extends BaseOperation implements Function {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      TupleEntry arguments = functionCall.getArguments();
      Tuple result = new Tuple();

      ProtoName.Name.Builder name = (ProtoName.Name.Builder) arguments.getObject(0);
      result.add(name.getFirstName());
      result.add(name.getLastName());
      functionCall.getOutputCollector().add(result);
    }
  }
}
