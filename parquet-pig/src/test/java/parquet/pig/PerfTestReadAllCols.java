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
package parquet.pig;

import java.io.File;


/**
 *
 * Uses directly loader and storer to bypass the scheduling overhead
 *
 * @author Julien Le Dem
 *
 */
public class PerfTestReadAllCols {

  public static void main(String[] args) throws Exception {
    StringBuilder results = new StringBuilder();
    String out = "target/PerfTestReadAllCols";
    File outDir = new File(out);
    if (outDir.exists()) {
      PerfTest2.clean(outDir);
    }
    PerfTest2.write(out);

    for (int i = 0; i < 5; i++) {

      PerfTest2.load(out, PerfTest2.COLUMN_COUNT, results);
      results.append("\n");
    }
    System.out.println(results);
  }


}
