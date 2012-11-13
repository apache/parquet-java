package redelm.pig;

import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import redelm.io.RecordConsumer;
import redelm.schema.MessageType;

public class TupleReadSupport extends ReadSupport<Tuple> {

  private Schema pigSchema;
  private MessageType requestedSchema;

  @Override
  public void initForRead(RedelmInputSplit redelmInputSplit, MessageType requestedSchema) {
    this.requestedSchema = requestedSchema;
    try {
      this.pigSchema = Utils.getSchemaFromString(redelmInputSplit.getPigSchema());
    } catch (ParserException e) {
        throw new RuntimeException("could not parse Pig schema", e);
    }

  }

  @Override
  public RecordConsumer newRecordConsumer(List<Tuple> destination) {
    return new TupleRecordConsumer(requestedSchema, pigSchema, destination);
  }

}
