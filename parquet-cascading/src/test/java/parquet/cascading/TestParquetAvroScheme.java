/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import avrotestclasses.Name;

/**
 * Created by schuman on 23/03/15.
 */
public class TestParquetAvroScheme {
    final String txtInputPath = "src/test/resources/names.txt";
    final String parquetInputPath = "target/test/ParquetAvroScheme/names-parquet-in";
    final String parquetOutputPath = "target/test/ParquetAvroScheme/names-parquet-out";
    final String txtOutputPath = "target/test/ParquetAvroScheme/names-txt-out";

    @Test
    public void testWrite() throws Exception {
        Path path = new Path(parquetOutputPath);
        JobConf jobConf = new JobConf();
        final FileSystem fs = path.getFileSystem(jobConf);
        if (fs.exists(path)) fs.delete(path, true);

        Scheme sourceScheme = new TextLine( new Fields( "first", "last" ) );
        Tap source = new Hfs(sourceScheme, txtInputPath);

        Scheme sinkScheme = new ParquetAvroScheme(Name.class);
        Tap sink = new Hfs(sinkScheme, parquetOutputPath);

        Pipe assembly = new Pipe( "namecp" );
        assembly = new Each(assembly, new PackAvroFunction());
        HadoopFlowConnector hadoopFlowConnector = new HadoopFlowConnector();
        Flow flow  = hadoopFlowConnector.connect("namecp", source, sink, assembly);

        flow.complete();

        assertTrue(fs.exists(new Path(parquetOutputPath)));
        assertTrue(fs.exists(new Path(parquetOutputPath + "/_SUCCESS")));
    }

    @Test
    public void testRead() throws Exception {
        doRead(new ParquetAvroScheme(Name.class));
    }

    /*@Test
    // TODO: make scheme work without requiring code generation
    public void testReadWithoutClass() throws Exception {
        doRead(new ParquetAvroScheme(GenericRecord.class));
    }*/

    private void doRead(Scheme sourceScheme) throws Exception {
        createFileForRead();

        Path path = new Path(txtOutputPath);
        final FileSystem fs = path.getFileSystem(new Configuration());
        if (fs.exists(path)) fs.delete(path, true);

        Tap source = new Hfs(sourceScheme, parquetInputPath);

        Scheme sinkScheme = new TextLine(new Fields("first", "last"));
        Tap sink = new Hfs(sinkScheme, txtOutputPath);

        Pipe assembly = new Pipe( "namecp" );
        assembly = new Each(assembly, new UnpackAvroFunction());
        Flow flow  = new HadoopFlowConnector().connect("namecp", source, sink, assembly);

        flow.complete();
        String result = FileUtils.readFileToString(new File(txtOutputPath + "/part-00000"));
        assertEquals("Alice\tPractice\nBob\tHope\nCharlie\tHorse\n", result);
    }


    private void createFileForRead() throws Exception {
        final Path fileToCreate = new Path(parquetInputPath+"/names.parquet");

        final Configuration conf = new Configuration();
        final FileSystem fs = fileToCreate.getFileSystem(conf);
        if (fs.exists(fileToCreate)) fs.delete(fileToCreate, true);

        AvroParquetWriter writer = new AvroParquetWriter(fileToCreate, Name.getClassSchema(), CompressionCodecName.UNCOMPRESSED, 100, 100);
        Name n1 = new Name();
        n1.setFirstName("Alice");
        n1.setLastName("Practice");
        Name n2 = new Name();
        n2.setFirstName("Bob");
        n2.setLastName("Hope");
        Name n3 = new Name();
        n3.setFirstName("Charlie");
        n3.setLastName("Horse");

        writer.write(n1);
        writer.write(n2);
        writer.write(n3);
        writer.close();
    }

    private static class PackAvroFunction extends BaseOperation implements Function {
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            TupleEntry arguments = functionCall.getArguments();
            Tuple result = new Tuple();

            Name name = new Name();
            name.setFirstName(arguments.getString(0));
            name.setLastName(arguments.getString(1));

            result.add(name);
            functionCall.getOutputCollector().add(result);
        }
    }

    private static class UnpackAvroFunction extends BaseOperation implements Function {
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            TupleEntry arguments = functionCall.getArguments();
            Tuple result = new Tuple();

            Name name = (Name) arguments.get(0);
            result.add(name.getFirstName());
            result.add(name.getLastName());
            functionCall.getOutputCollector().add(result);
        }
    }
}
