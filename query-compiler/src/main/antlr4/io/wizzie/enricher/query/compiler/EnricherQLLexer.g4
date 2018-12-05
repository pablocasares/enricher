lexer grammar EnricherQLLexer;

// SKIP

WS: [ \r\n\t]+ -> skip;

// Keywords

BY: 'BY';
ENRICH: 'ENRICH';
FROM: 'FROM';
GLOBAL: 'GLOBAL';
INSERT: 'INSERT';
INTO: 'INTO';
JOIN: 'JOIN';
PARTITION: 'PARTITION';
SELECT: 'SELECT';
STREAM: 'STREAM';
TABLE: 'TABLE';
USING: 'USING';
WITH: 'WITH';



// Identifier

ID: ID_LITERAL;
STAR: '*';
COMMA: ',';

fragment ID_LITERAL : [a-z_A-Z] ([a-zA-Z0-9]|HYPHEN|UNDERSCORE)*;

fragment HYPHEN: '-';
fragment UNDERSCORE: '_';
