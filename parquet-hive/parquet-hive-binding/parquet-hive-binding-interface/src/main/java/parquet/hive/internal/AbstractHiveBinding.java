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
package parquet.hive.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.util.StringUtils;

import parquet.hive.HiveBinding;

/**
 * Common code among implementations of {@link parquet.hive.HiveBinding HiveBinding}.
 */
public abstract class AbstractHiveBinding implements HiveBinding {
  private static final List<String> virtualColumns;
  
  static {
    List<String> vcols =  new ArrayList<String>();
    vcols.add("INPUT__FILE__NAME");
    vcols.add("BLOCK__OFFSET__INSIDE__FILE");
    vcols.add("ROW__OFFSET__INSIDE__BLOCK");
    vcols.add("RAW__DATA__SIZE");
    virtualColumns = Collections.unmodifiableList(vcols);
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public List<String> getColumns(final String columns) {
    final List<String> result = (List<String>) StringUtils.getStringCollection(columns);
    result.removeAll(virtualColumns);
    return result;
  }

}
