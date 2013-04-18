/**
 * Copyright 2013 Criteo.
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
package parquet.hive.convert;

import java.util.Map;

import org.apache.hadoop.io.MapWritable;

import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;


/**
 *
 * A MapWritableReadSupport
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
public class MapWritableRecordConverter extends RecordMaterializer<MapWritable> {

  private final MapWritableGroupConverter root;

  public MapWritableRecordConverter(final GroupType requestedSchema, final Map<String, String> keyValueMetaData, final MessageType fileSchema) {
    this.root = new MapWritableGroupConverter(requestedSchema);

  }

  @Override
  public MapWritable getCurrentRecord() {
    return root.getCurrentMap();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }

}
