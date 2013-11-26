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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public interface HiveBinding {

  /**
   * From a string which columns names (including hive column), return a list
   * of string columns
   *
   * @param comma separated list of columns
   * @return list with virtual columns removed
   */
  public List<String> removeVirtualColumns(final String columns);
  
  /**
   * Processes the JobConf object pushing down projections and filters.
   * We are going to get the Table from a partition in order to get all the
   * aliases from it. Once we have them, we take a look at the different
   * columns needed for each of them, and we update the job by appending
   * these columns.
   *
   * At the end, the new JobConf will contain all the wanted columns.
   *
   * @param jobConf
   * @param path
   * @return jobConf which to be used for reading Parquet files
   * @throws IOException
   */
  public JobConf pushProjectionsAndFilters(final JobConf jobConf, final Path path) throws IOException;
}
