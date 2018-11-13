package io.wizzie.enricher.query;

import io.wizzie.enricher.query.compiler.EnricherQLLexer;
import io.wizzie.enricher.query.compiler.EnricherQLParser;
import io.wizzie.enricher.query.internal.EnricherErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EnricherQLUnitTest {

    @Test
    public void InsertIntoShouldWork() {
        String query = "INSERT INTO STREAM output";

        CharStream inputStream = CharStreams.fromString(query);

        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);

        assertEquals(query.replaceAll("\\s+", ""), parser.outputStatement().getText());

    }

    @Test
    public void JoinWithStreamShouldWork() {
        String query = "JOIN SELECT * FROM STREAM input USING jClass";

        CharStream inputStream = CharStreams.fromString(query);

        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);

        assertEquals(query.replaceAll("\\s+", ""), parser.joinStatement().getText());

    }

    @Test
    public void JoinWithTableShouldWork() {
        String query = "JOIN SELECT * FROM TABLE input USING jClass";

        CharStream inputStream = CharStreams.fromString(query);

        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);

        assertEquals(query.replaceAll("\\s+", ""), parser.joinStatement().getText());

    }

    @Test
    public void JoinWithGlobalTableShouldWork() {
        String query = "JOIN SELECT * FROM TABLE input USING jClass";

        CharStream inputStream = CharStreams.fromString(query);

        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);

        assertEquals(query.replaceAll("\\s+", ""), parser.joinStatement().getText());

    }

    @Test
    public void JoinWithByKeyShouldWork() {
        String query = "JOIN SELECT * FROM TABLE input BY key USING jClass";

        CharStream inputStream = CharStreams.fromString(query);

        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);

        assertEquals(query.replaceAll("\\s+", ""), parser.joinStatement().getText());

    }

    @Test
    public void JoinWithoutByKeyShouldWork() {

        String query = "JOIN SELECT * FROM STREAM input USING jClass";

        CharStream inputStream = CharStreams.fromString(query);

        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);

        assertEquals(query.replaceAll("\\s+", ""), parser.joinStatement().getText());

    }

    @Test
    public void EnricherWithShouldWork() {
        String query = "ENRICH WITH pclass1";

        CharStream inputStream = CharStreams.fromString(query);

        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);

        assertEquals(query.replaceAll("\\s+", ""), parser.enrichStatement().getText());
    }

    @Test
    public void SimpleSelectShouldWork() {

        String query = "SELECT * FROM STREAM input " +
                "JOIN SELECT a,b FROM STREAM input2 USING jpackageClass " +
                "INSERT INTO STREAM output";

        CharStream inputStream = CharStreams.fromString(query);

        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);

        assertEquals(query.replaceAll("\\s+", ""), parser.query().getText());

    }

    @Test
    public void ComplexSelectShouldWork() {

        String query = "SELECT * FROM STREAM input " +
                "JOIN SELECT a,b,c FROM STREAM input2 USING jClass1 " +
                "JOIN SELECT * FROM TABLE input3 USING jClass2 "+
                "JOIN SELECT x,y FROM TABLE input4 USING jClass3 " +
                "INSERT INTO TABLE output";

        CharStream inputStream = CharStreams.fromString(query);

        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);

        assertEquals(query.replaceAll("\\s+", ""), parser.query().getText());

    }

    @Test
    public void PartitionOutputShouldWork() {
        String query = "SELECT * FROM STREAM input INSERT INTO TABLE output PARTITION BY myKey";

        CharStream inputStream = CharStreams.fromString(query);

        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);

        assertEquals(query.replaceAll("\\s+", ""), parser.query().getText());
    }
}
