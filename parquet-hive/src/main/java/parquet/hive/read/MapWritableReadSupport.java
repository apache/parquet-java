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
package parquet.hive.read;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;

import parquet.hadoop.api.ReadSupport;
import parquet.hive.convert.MapWritableRecordConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

/**
*
* A MapWritableReadSupport
*
*
* @author Mickaël Lacour <m.lacour@criteo.com>
*
*/
public class MapWritableReadSupport extends ReadSupport<MapWritable> {

    @Override
    public parquet.hadoop.api.ReadSupport.ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
        return new ReadContext(fileSchema);
    }

    @Override
    public RecordMaterializer<MapWritable> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema,
            parquet.hadoop.api.ReadSupport.ReadContext readContext) {
        return new MapWritableRecordConverter(readContext.getRequestedSchema(), keyValueMetaData, fileSchema);
    }
}
