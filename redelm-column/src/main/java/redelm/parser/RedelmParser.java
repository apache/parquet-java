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

  public static MessageType parseMessageType(String input) throws RedelmParserException {
    return parseMessageType(new ANTLRStringStream(input));
  }

  public static MessageType parseMessageTypeFromFile(String fileName) throws RedelmParserException, IOException {
    return parseMessageType(new ANTLRFileStream(fileName));
  }

  private static MessageType parseMessageType(ANTLRStringStream stream) throws RedelmParserException {
    RedelmMessageLexer lexer = new RedelmMessageLexer(stream);
    RedelmMessageParser parser = new RedelmMessageParser(new CommonTokenStream(lexer));
    MessageType ret;
    try {
      ret = parser.message();
    } catch (RecognitionException e) {
      throw new RedelmParserException(e);
    }
    if (ret == null) {
      throw new RedelmParserException("Attempted to parse object, result was null: " + stream.toString());
    }
    return ret;
  }

  public static class RedelmParserException extends Exception {
    private static final long serialVersionUID = 8058462449549308200L;

    public RedelmParserException() {
    }

    public RedelmParserException(String msg) {
      super(msg);
    }

    public RedelmParserException(String msg, Exception e) {
      super(msg, e);
    }

    public RedelmParserException(Exception e) {
      super(e);
    }
  }
}