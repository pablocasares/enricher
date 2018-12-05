parser grammar EnricherQLParser;

options {
    language = Java;
    tokenVocab = EnricherQLLexer;
}

query
    : selectStatement
    ;

selectStatement
    : SELECT dimensionList FROM STREAM streamList (joinStatement)* (enrichStatement)* outputStatement
    ;

joinStatement
    locals [
        String _dimensionList = "*",
        boolean _isTable = false,
        boolean _isGlobalTable = false,
        String _partitionKey = "__KEY",
        String _joinerName = "",
        String _streamName = ""
    ]
    : JOIN (selectStreamOrTableStatement {
        $ctx._isTable = $ctx.selectStreamOrTableStatement()._isTable;
    } | selectGlobalTableStatement {
        $ctx._isTable = true;
        $ctx._isGlobalTable = true;
    }) id {
        $ctx._streamName = $id.text;
    } (PARTITION? BY partitionKey {
        $ctx._partitionKey = $partitionKey.text;
    })? USING joinerName {
        $ctx._joinerName = $joinerName.text;
    }
    ;

selectGlobalTableStatement
    : FROM GLOBAL TABLE
    ;

selectStreamOrTableStatement
    locals [
        boolean _isTable = false;
    ]
    : SELECT dimensionList FROM streamOrTable {
        $ctx._isTable = $streamOrTable.text.equals("TABLE");
    }
    ;

enrichStatement
    locals [
        String _enricherName = ""
    ]
    : ENRICH WITH enricherName {
        $ctx._enricherName = $enricherName.text;
    }
    ;

outputStatement
    locals [
        boolean _isTable = false,
        String _streamName = "",
        String _partitionKey = "__KEY"
    ]
    : INSERT INTO streamOrTable {
        if ($streamOrTable.text.equals("TABLE")) $ctx._isTable = true;
    } id {
        $ctx._streamName = $id.text;
    } (PARTITION? BY partitionKey {
        $ctx._partitionKey = $partitionKey.text;
    })?
    ;

// Lists

dimensionList: ('*'| id (',' id)*);

streamList: id (',' id)*;

// Identifiers

enricherName: id;

joinerName: id;

partitionKey: id;

id: ID;

// keywords
streamOrTable
    : STREAM
    | TABLE
    ;