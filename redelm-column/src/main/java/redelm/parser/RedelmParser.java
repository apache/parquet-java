package redelm.parser;

import java.io.IOException;

import org.antlr.runtime.ANTLRFileStream;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import redelm.Log;
import redelm.schema.MessageType;

public class RedelmParser {
  private static final Log LOG = Log.getLog(RedelmParser.class);

  private RedelmParser() {}

  public static MessageType parse(String input) throws RecognitionException {
    return parse(new ANTLRStringStream(input));
  }

  public static MessageType parseFile(String fileName) throws RecognitionException, IOException {
    return parse(new ANTLRFileStream(fileName));
  }

  private static MessageType parse(ANTLRStringStream stream) throws RecognitionException {
    RedelmMessageLexer lexer = new RedelmMessageLexer(stream);
    RedelmMessageParser parser = new RedelmMessageParser(new CommonTokenStream(lexer));
    MessageType ret = parser.message();
    if (ret == null) {
      LOG.warn("Attempted to parse object, result was null: " + stream.toString());
    }
    return ret;
  }
}