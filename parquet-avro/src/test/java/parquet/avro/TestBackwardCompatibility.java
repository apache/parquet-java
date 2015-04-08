/**
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
package parquet.avro;

import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import parquet.hadoop.ParquetReader;

public class TestBackwardCompatibility {

  @Test
  public void testStringCompatibility() throws IOException {
    // some older versions of Parquet used avro.schema instead of
    // parquet.avro.schema and didn't annotate binary with UTF8 when the type
    // was converted from an Avro string. this validates that the old read
    // schema is recognized and used to read the file as expected.
    Path testFile = new Path(Resources.getResource("strings-2.parquet").getFile());
    Configuration conf = new Configuration();
    ParquetReader<GenericRecord> reader = AvroParquetReader
        .builder(new AvroReadSupport<GenericRecord>(), testFile)
        .withConf(conf)
        .build();
    GenericRecord r;
    while ((r = reader.read()) != null) {
      Assert.assertTrue("Should read value into a String",
          r.get("text") instanceof String);
    }
  }

}
