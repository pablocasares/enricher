package io.wizzie.enricher.query.internal;

import io.wizzie.enricher.query.exception.EnricherParserException;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public class EnricherErrorListener extends BaseErrorListener {

    public static EnricherErrorListener INSTANCE = new EnricherErrorListener();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
        throw new EnricherParserException("You have an error in yout EnricherQL at line " + line + ": " + charPositionInLine + ", " + msg);
    }

}
