package io.wizzie.enricher.query.antlr4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Select {

    List<String> dimensions = new ArrayList<>();
    List<Stream> streams = new ArrayList<>();

    public Select() {
        this(Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }

    public Select(List<String> dimensions, List<Stream> streams) {
        this.dimensions = checkNotNull(dimensions, "\"dimensions\" attribute is required");
        this.streams = checkNotNull(streams, "\"streams\" attribute is required");
    }

    public void setDimensions(List<String> newDimensionsList) {
        dimensions = newDimensionsList;
    }

    public void addNewDimension(String dimension) {
        dimensions.add(dimension);
    }

    public void addStream(Stream newStram) {
        streams.add(newStram);
    }

    public void setStreams(List<Stream> newStreamsList) {
        streams = newStreamsList;
    }

    public List<String> getDimensions() {
        return dimensions;
    }

    public List<Stream> getStreams() {
        return streams;
    }

    public void validate() {
        checkNotNull(dimensions, "\"dimensions\" attribute is required");
        checkNotNull(streams, "\"streams\" attribute is required");

        streams.forEach(Stream::validate);
    }

}
