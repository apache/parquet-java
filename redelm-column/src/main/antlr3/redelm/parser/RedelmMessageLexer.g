lexer grammar RedelmMessageLexer;

@header {
package redelm.parser;
}

@lexer::members {
  @Override
  public void reportError(RecognitionException e) {
    throw new RuntimeException("Lexer error encountered", e);
  }
}

WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ { $channel = HIDDEN; };

LEFT_CURLY : '{';

RIGHT_CURLY : '}';

SEMI_COLON : ';';

fragment A : 'a' | 'A';
fragment B : 'b' | 'B';
fragment C : 'c' | 'C';
fragment D : 'd' | 'D';
fragment E : 'e' | 'E';
fragment F : 'f' | 'F';
fragment G : 'g' | 'G';
fragment H : 'h' | 'H';
fragment I : 'i' | 'I';
fragment J : 'j' | 'J';
fragment K : 'k' | 'K';
fragment L : 'l' | 'L';
fragment M : 'm' | 'M';
fragment N : 'n' | 'N';
fragment O : 'o' | 'O';
fragment P : 'p' | 'P';
fragment Q : 'q' | 'Q';
fragment R : 'r' | 'R';
fragment S : 's' | 'S';
fragment T : 't' | 'T';
fragment U : 'u' | 'U';
fragment V : 'v' | 'V';
fragment W : 'w' | 'W';
fragment X : 'x' | 'X';
fragment Y : 'y' | 'Y';
fragment Z : 'z' | 'Z';

REQUIRED : R E Q U I R E D;
OPTIONAL : O P T I O N A L;
REPEATED: R E P E A T E D;

GROUP : G R O U P;
LONG : I N T '64';
STRING : S T R I N G;
FLOAT : F L O A T;
DOUBLE : D O U B L E;
BOOLEAN : B O O L E A N;

MESSAGE : M E S S A G E;

fragment ULETTER : 'A'..'Z';
fragment LETTER : 'a'..'z' | ULETTER;
fragment COL_LETTER : ':' | '_' | LETTER;

NAME : LETTER COL_LETTER*;

