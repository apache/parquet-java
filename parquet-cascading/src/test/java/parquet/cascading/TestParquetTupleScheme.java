/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.cascading;

import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import parquet.hadoop.ParquetInputFormat;
import java.net.URL;

public class TestParquetTupleScheme {
  Tap tap;
  FlowProcess fp;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new Configuration();
    final Path parquetPath = new Path("target/test/thrift/TestInputOutputFormat/parquet");
    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    System.out.println(fileSystem);
    URL url = Thread.currentThread().getContextClassLoader().getResource("part-m-00000.gz.parquet");
    tap = new Hfs(new TextDelimited(true, "\t"), url.getPath());
    fp = mock(FlowProcess.class);
    when(fp.getConfigCopy()).thenReturn(new JobConf());
  }

  @Test
  public void testReadSupportSetting() {
    ParquetTupleScheme scheme = new ParquetTupleScheme(Fields.ALL);
    FlowProcess<JobConf> fp = mock(FlowProcess.class);
    Tap<JobConf, RecordReader, OutputCollector> tap = mock(Tap.class);
    JobConf jobConf = new JobConf();

    scheme.sourceConfInit(fp, tap, jobConf);
    System.out.println(scheme.getSourceFields());
    assertEquals(ParquetInputFormat.getReadSupportClass(jobConf), TupleReadSupport.class);
  }

  @Test
  public void testRetrieveAllFields() throws Exception {
    ParquetTupleScheme scheme = new ParquetTupleScheme(Fields.ALL);
    scheme.sourceConfInit(fp,
            tap,
            new JobConf());
    Fields fs = scheme.retrieveSourceFields(fp, tap);
    assertEquals("persons", fs.get(0).toString());
  }

  @Test
  public void testRetrieveDefaultAllFields() throws Exception {
    ParquetTupleScheme scheme = new ParquetTupleScheme();
    scheme.sourceConfInit(fp,
            tap,
            new JobConf());
    Fields fs = scheme.retrieveSourceFields(fp, tap);
    assertEquals("persons", fs.get(0).toString());
  }

  @Test
  public void testRetrieveNoneFields() throws Exception {
    ParquetTupleScheme scheme = new ParquetTupleScheme(Fields.NONE);
    scheme.sourceConfInit(fp,
            tap,
            new JobConf());
    Fields fs = scheme.retrieveSourceFields(fp, tap);
    assertEquals(0, fs.size());
  }
}
