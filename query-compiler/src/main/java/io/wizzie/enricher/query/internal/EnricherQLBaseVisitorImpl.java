package io.wizzie.enricher.query.internal;

import io.wizzie.enricher.query.antlr4.Join;
import io.wizzie.enricher.query.antlr4.Query;
import io.wizzie.enricher.query.antlr4.Select;
import io.wizzie.enricher.query.antlr4.Stream;
import io.wizzie.enricher.query.compiler.EnricherQLParser;
import io.wizzie.enricher.query.compiler.EnricherQLParserBaseVisitor;
import org.antlr.v4.runtime.RuleContext;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class EnricherQLBaseVisitorImpl extends EnricherQLParserBaseVisitor {


    @Override
    public Query visitQuery(EnricherQLParser.QueryContext queryContext) {

        List<String> selectDimensions = queryContext.selectStatement().dimensionList().id().stream().map(RuleContext::getText).collect(Collectors.toList());

        if (selectDimensions.isEmpty())
            selectDimensions = Collections.singletonList("*");


        List<Stream> selectInputStreams = queryContext.selectStatement().streamList().id().stream().map(id -> new Stream(id.getText())).collect(Collectors.toList());

        List<Join> joins = queryContext.selectStatement().joinStatement().stream().map(joinContext -> {

            List<String> joinDimensions = Collections.emptyList();

            if (!joinContext._isGlobalTable) {
                if (!joinContext.selectStreamOrTableStatement().streamOrTable.isEmpty()) {
                    joinDimensions = joinContext.selectStreamOrTableStatement().dimensionList().id().stream().map(id -> id.getText()).collect(Collectors.toList());

                    if(joinDimensions.isEmpty())
                        joinDimensions = Collections.singletonList("*");
                }
            }

            Stream stream = new Stream(joinContext._streamName, joinContext._isTable, joinContext._isGlobalTable);

            return new Join(stream, joinContext._joinerName, joinDimensions, joinContext._partitionKey);

        }).collect(Collectors.toList());


        List<String> enrichWiths = queryContext.selectStatement().enrichStatement().stream().map(enrichContext -> enrichContext._enricherName).collect(Collectors.toList());

        EnricherQLParser.OutputStatementContext outputContext = queryContext.selectStatement().outputStatement();

        Stream output = new Stream(outputContext._streamName, outputContext._isTable);

        String outputPartitionKey = outputContext._partitionKey;

        return new Query(new Select(selectDimensions, selectInputStreams), output, joins, enrichWiths, outputPartitionKey);
    }
}
