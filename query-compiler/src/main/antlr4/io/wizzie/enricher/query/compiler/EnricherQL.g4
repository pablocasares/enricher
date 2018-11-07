grammar EnricherQL;

options {
    language = Java;
}

query: SELECT dimensionList FROM STREAM streamList (query_join)* (query_enrich_with)* query_output;

query_join
    locals [
    boolean isTable = false,
    boolean isGlobalTable = false]
    : JOIN (
        SELECT dimensionList FROM streamOrTable {
        if ($streamOrTable.text.equals("TABLE")) $ctx.isTable = true;
    } | FROM GLOBAL TABLE {
            $ctx.isTable = true;
            $ctx.isGlobalTable = true;
        }
    ) id (PARTITION? BY partitionKey)? USING className;


query_enrich_with: ENRICH WITH className;

query_output: INSERT INTO streamOrTable id (PARTITION BY partitionKey)?;

streamOrTable
    : STREAM
    | GLOBAL? TABLE
    ;

dimensionList: (dimWildcard | (id (',' id)*));
streamList: id (',' id)*;
className: id ('.' id)*;

partitionKey: ID;
id: ID;

dimWildcard: STAR;

STAR: '*';

// Keywords
SELECT: S E L E C T;
FROM: F R O M;
JOIN: J O I N;
TABLE: T A B L E;
STREAM: S T R E A M;
USING: U S I N G;
INSERT: I N S E R T;
INTO: I N T O;
ENRICH: E N R I C H;
WITH: W I T H;
BY: B Y;
PARTITION: P A R T I T I O N;
GLOBAL: G L O B A L;

ID : ([a-zA-Z]|UNDERSCORE) ([a-zA-Z0-9]|HYPHEN|UNDERSCORE)*;


WS: [ \r\n\t] -> skip;

fragment HYPHEN: '-';
fragment UNDERSCORE: '_';
fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
