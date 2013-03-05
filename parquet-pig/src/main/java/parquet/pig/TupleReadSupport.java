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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import parquet.Log;
import parquet.hadoop.ReadSupport;
import parquet.io.convert.RecordConverter;
import parquet.pig.convert.TupleConverter;
import parquet.pig.convert.TupleRecordConverter;
import parquet.schema.MessageType;

/**
 * Read support for Pig Tuple
 * a Pig MetaDataBlock is expected in the initialization call
 *
 * @author Julien Le Dem
 *
 */
public class TupleReadSupport extends ReadSupport<Tuple> {
  private static final Log LOG = Log.getLog(TupleReadSupport.class);

  private static final PigSchemaConverter schemaConverter = new PigSchemaConverter();

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordConverter<Tuple> initForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fielSchema,
      MessageType requestedSchema) {
    PigMetaData pigMetaData = PigMetaData.fromMetaData(keyValueMetaData);
    try {
      Schema pigSchema = schemaConverter.filter(
          Utils.getSchemaFromString(pigMetaData.getPigSchema()),
          requestedSchema);
      return new TupleRecordConverter(requestedSchema, pigSchema);
    } catch (ParserException e) {
      throw new RuntimeException("could not parse Pig schema: " + pigMetaData.getPigSchema(), e);
    }
  }

}
