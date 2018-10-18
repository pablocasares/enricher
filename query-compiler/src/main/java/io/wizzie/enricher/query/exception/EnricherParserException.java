package io.wizzie.enricher.query.exception;

public class EnricherParserException extends RuntimeException {

    public EnricherParserException() {super();}

    public EnricherParserException(String message) {super(message);}

    public EnricherParserException(String message, Throwable throwable) {super(message, throwable);}

    public EnricherParserException(Throwable throwable) {super(throwable);}
}
