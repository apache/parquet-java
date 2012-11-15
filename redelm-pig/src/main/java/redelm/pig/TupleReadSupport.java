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
