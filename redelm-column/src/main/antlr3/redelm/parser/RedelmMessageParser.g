parser grammar RedelmMessageParser;

options {
    tokenVocab=RedelmMessageLexer;
}

tokens {
    LEFT_CURLY;
    RIGHT_CURLY;
    SEMI_COLON;
    REQUIRED;
    OPTIONAL;
    REPEATED;
    GROUP;
    LONG;
    STRING;
    FLOAT;
    DOUBLE;
    MESSAGE;
    NAME;
    LINE;
}

@header {
package redelm.parser;

import java.util.List;
import java.util.ArrayList;

import redelm.schema.Type;
import redelm.schema.Type.Repetition;
import redelm.schema.GroupType;
import redelm.schema.PrimitiveType;
import redelm.schema.PrimitiveType.Primitive;
import redelm.schema.MessageType;
}

@parser::members {
  @Override
  public void reportError(RecognitionException e) {
    throw new RuntimeException("Parser error encountered", e);
  }
}

repetition returns[Repetition rep] : REQUIRED { $rep = Repetition.REQUIRED; }
                                   | OPTIONAL { $rep = Repetition.OPTIONAL; }
                                   | REPEATED { $rep = Repetition.REPEATED; };

primitive_type returns[Primitive primitive] : LONG { $primitive = Primitive.INT64; }
                                            | STRING { $primitive = Primitive.STRING; }
                                            | FLOAT { $primitive = Primitive.FLOAT; }
                                            | DOUBLE { $primitive = Primitive.FLOAT; };

line returns[Type type] : repetition primitive_type NAME SEMI_COLON+
     {
         $type = new PrimitiveType($repetition.rep, $primitive_type.primitive, $NAME.text);
     }
     | repetition GROUP NAME LEFT_CURLY many_lines RIGHT_CURLY SEMI_COLON*
     {
         $type = new GroupType($repetition.rep, $NAME.text, $many_lines.fields);
     }
;

many_lines returns[Type[\] fields]
@init {
    List<Type> fieldsList = new ArrayList<Type>();
}
: (line { fieldsList.add($line.type); } )+ { $fields = fieldsList.toArray(new Type[0]); };

message returns[MessageType value] : MESSAGE NAME LEFT_CURLY many_lines RIGHT_CURLY { $value = new MessageType($NAME.text, $many_lines.fields); };
