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
package org.apache.parquet.tools.command;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Registry {
  public static Map<String,Class<? extends Command>> registry;

  static {
    registry = new LinkedHashMap<String,Class<? extends Command>>();
    registry.put("cat", CatCommand.class);
    registry.put("head", HeadCommand.class);
    registry.put("schema", ShowSchemaCommand.class);
    registry.put("meta", ShowMetaCommand.class);
    registry.put("dump", DumpCommand.class);
    registry.put("merge", MergeCommand.class);
    registry.put("rowcount", RowCountCommand.class);
    registry.put("size", SizeCommand.class);
    registry.put("column-index", ColumnIndexCommand.class);
    registry.put("prune", PruneColumnsCommand.class);
    registry.put("column-size", ColumnSizeCommand.class);
    registry.put("trans-compression", TransCompressionCommand.class);
    registry.put("masking", ColumnMaskingCommand.class);
  }

  public static Map<String,Command> allCommands() {
    Map<String,Command> results = new LinkedHashMap<String,Command>();
    for (Map.Entry<String,Class<? extends Command>> entry : registry.entrySet()) {
      try {
        results.put(entry.getKey(), entry.getValue().newInstance());
      } catch (Exception ex) {
      }
    }

    return results;
  }

  public static Command getCommandByName(String name) {
    Class<? extends Command> clazz = registry.get(name);
    if (clazz == null) {
      return null;
    }

    try {
      return clazz.newInstance();
    } catch (Exception ex) {
      return null;
    }
  }
}
