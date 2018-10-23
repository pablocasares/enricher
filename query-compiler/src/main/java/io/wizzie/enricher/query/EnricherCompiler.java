package io.wizzie.enricher.query;

import io.wizzie.enricher.query.antlr4.Query;
import io.wizzie.enricher.query.compiler.EnricherQLLexer;
import io.wizzie.enricher.query.compiler.EnricherQLParser;
import io.wizzie.enricher.query.compiler.EnricherQLVisitor;
import io.wizzie.enricher.query.internal.EnricherErrorListener;
import io.wizzie.enricher.query.internal.EnricherQLBaseVisitorImpl;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class EnricherCompiler {

    public static Query parse(String source) {

        ANTLRInputStream input = new ANTLRInputStream(source);
        EnricherQLLexer lexer = new EnricherQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);
        ParseTree tree = parser.query();

        EnricherQLVisitor<Query> eval = new EnricherQLBaseVisitorImpl();

        return eval.visit(tree);
    }
}
