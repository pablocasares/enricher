package io.wizzie.enricher.query.internal;

import io.wizzie.enricher.query.antlr4.Join;
import io.wizzie.enricher.query.antlr4.Query;
import io.wizzie.enricher.query.antlr4.Select;
import io.wizzie.enricher.query.antlr4.Stream;
import io.wizzie.enricher.query.compiler.EnricherQLBaseVisitor;
import io.wizzie.enricher.query.compiler.EnricherQLParser;
import io.wizzie.enricher.query.utils.Constants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class EnricherQLBaseVisitorImpl extends EnricherQLBaseVisitor {

    @Override
    public Query visitQuery(EnricherQLParser.QueryContext ctx) {
        List<String> selectDimensions = ctx.dimensionList().id().stream().map(id -> id.getText()).collect(Collectors.toList());

        if (selectDimensions.isEmpty())
            selectDimensions = Collections.singletonList("*");

        List<Stream> selectInputStreams = ctx.streamList().id().stream().map(id -> new Stream(id.getText())).collect(Collectors.toList());

        List<Join> joins = ctx.query_join().stream().map(joinContext -> {

            List<String> joinDimensions = Collections.emptyList();

            if (!joinContext.isGlobalTable) {
                if (!joinContext.streamOrTable().isEmpty()) {
                    joinDimensions = joinContext.dimensionList().id().stream().map(id -> id.getText()).collect(Collectors.toList());

                    if(joinDimensions.isEmpty())
                        joinDimensions = Collections.singletonList("*");
                }
            }

            String className = joinContext.className().getText();
            String joinStream = joinContext.id().getText();

            EnricherQLParser.PartitionKeyContext partitionKeyContext;

            String partitionKey = ( partitionKeyContext = joinContext.partitionKey()) != null ? partitionKeyContext.getText() : Constants.__KEY;

            Stream stream = new Stream(joinStream, joinContext.isTable, joinContext.isGlobalTable);

            return new Join(stream, className, joinDimensions, partitionKey);
        }).collect(Collectors.toList());

        List<String> enrichWiths = ctx.query_enrich_with().stream().map(enrichContext -> new String(enrichContext.className().getText())).collect(Collectors.toList());

        Stream output = new Stream(ctx.query_output().id().getText(), ctx.query_output().streamOrTable().getText().matches("TABLE"));

        String outputPartitionKey = null;

        if (ctx.query_output().partitionKey() != null) {
            outputPartitionKey = ctx.query_output().partitionKey().getText();
        }

        return new Query(new Select(selectDimensions, selectInputStreams), output, joins, enrichWiths, outputPartitionKey);
    }

}
