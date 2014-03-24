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
package parquet.hadoop;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestParquetInputSplit {

    @Test
    public void testStringCompression() {
      String[] strings = {"this is a string",
         "this is a string with a \n newline",
         "a:chararray, b:{t:(c:chararray, d:chararray)}",
         "message pig_schema {\n" +
         "  optional binary a;\n" +
         "  optional group b {\n" +
         "    repeated group t {\n" +
         "      optional binary c;\n" +
         "      optional binary d;\n" +
         "    }\n" +
         "  }\n" +
         "}\n"
         };
      ParquetInputSplit split = new ParquetInputSplit();
      for (String s : strings) {
        String cs = split.compressString(s);
        String uncs = split.decompressString(cs);
        assertEquals("strings should be same after decompressing", s, uncs);
      }
    }
}
