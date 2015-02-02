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
  public List<String> getColumns(final String columns);
  
  /**
   * Processes the JobConf object pushing down projections and filters.
   *
   * We are going to get the Table from a partition in order to get all the
   * aliases from it. Once we have them, we take a look at the different
   * columns needed for each of them, and we update the job by appending
   * these columns.
   *
   * The JobConf is modified and therefore is cloned first to ensure
   * other owners are not impacted by the changes here. This is a standard
   * practice when modifying JobConf objects in InputFormats, for example
   * HCatalog does this.
   *
   * @param jobConf 
   * @param path
   * @return cloned jobConf which can be used to read Parquet files
   * @throws IOException
   */
  public JobConf pushProjectionsAndFilters(final JobConf jobConf, final Path path) throws IOException;
}
