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
package redelm.pig;

import java.util.List;

import redelm.hadoop.MetaDataBlock;
import redelm.hadoop.ReadSupport;
import redelm.io.RecordConsumer;
import redelm.parser.MessageTypeParser;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

public class TupleReadSupport extends ReadSupport<Tuple> {
  private static final long serialVersionUID = 1L;

  private Schema pigSchema;
  private String requestedSchema;

  @Override
  public void initForRead(List<MetaDataBlock> metaDataBlocks, String requestedSchema) {
    this.requestedSchema = requestedSchema;
    PigMetaData pigMetaData = PigMetaData.fromMetaDataBlocks(metaDataBlocks);
    try {
      this.pigSchema = Utils.getSchemaFromString(pigMetaData.getPigSchema());
    } catch (ParserException e) {
      throw new RuntimeException("could not parse Pig schema: " + pigMetaData.getPigSchema(), e);
    }
  }

  @Override
  public RecordConsumer newRecordConsumer(List<Tuple> destination) {
    return new TupleRecordConsumer(
        MessageTypeParser.parseMessageType(requestedSchema),
        pigSchema,
        destination);
  }

}
