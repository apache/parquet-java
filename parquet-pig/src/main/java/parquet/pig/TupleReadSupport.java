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


import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import parquet.Log;
import parquet.hadoop.ReadSupport;
import parquet.io.RecordMaterializer;
import parquet.parser.MessageTypeParser;
import parquet.pig.converter.MessageConverter;
import parquet.schema.MessageType;

/**
 * Read support for Pig Tuple
 * a Pig MetaDataBlock is expected in the initialization call
 *
 * @author Julien Le Dem
 *
 */
public class TupleReadSupport extends ReadSupport<Tuple> {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = Log.getLog(TupleReadSupport.class);

  private static final PigSchemaConverter schemaConverter = new PigSchemaConverter();

  private Schema pigSchema;
  private String requestedSchema;

  /**
   * {@inheritDoc}
   */
  @Override
  public void initForRead(Map<String, String> keyValueMetaData, String requestedSchema) {
    this.requestedSchema = requestedSchema;
    PigMetaData pigMetaData = PigMetaData.fromMetaDataBlocks(keyValueMetaData);
    try {
      this.pigSchema = schemaConverter.filter(
          Utils.getSchemaFromString(pigMetaData.getPigSchema()),
          MessageTypeParser.parseMessageType(requestedSchema));
    } catch (ParserException e) {
      throw new RuntimeException("could not parse Pig schema: " + pigMetaData.getPigSchema(), e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordMaterializer<Tuple> newRecordConsumer() {
    MessageType parquetSchema = MessageTypeParser.parseMessageType(requestedSchema);
    MessageConverter converter = newParsingTree(parquetSchema, pigSchema);
    if (Log.DEBUG) LOG.debug("assembled converter: " + converter);
    return converter.newRecordConsumer();
//    return new TupleRecordConsumer(parquetSchema, pigSchema);
  }

  private MessageConverter newParsingTree(MessageType parquetSchema, Schema pigSchema) {
    try {
      return new MessageConverter(parquetSchema, pigSchema);
    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }

}
