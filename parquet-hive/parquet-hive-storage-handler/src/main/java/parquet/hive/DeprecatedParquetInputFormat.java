/**
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
package parquet.hive;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * Deprecated name of the parquet-hive input format. This class exists
 * simply to provide backwards compatibility with users who specified
 * this name in the Hive metastore. All users should now use
 * {@link MapredParquetInputFormat MapredParquetInputFormat}
 */
@Deprecated
public class DeprecatedParquetInputFormat extends MapredParquetInputFormat {

  public DeprecatedParquetInputFormat() {
    super();
  }

  public DeprecatedParquetInputFormat(final InputFormat<Void, ArrayWritable> realInputFormat) {
    super(realInputFormat);
  }
}
