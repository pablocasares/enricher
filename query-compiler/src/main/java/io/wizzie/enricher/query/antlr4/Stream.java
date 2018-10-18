package io.wizzie.enricher.query.antlr4;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Stream {

    String name;
    boolean isTable = false;

    public Stream(String name) {
        this(name, false);
    }

    public Stream(String name, boolean isTable) {
        this.name = checkNotNull(name, "\"name\" attribute is required");
        this.isTable = checkNotNull(isTable, "\"isTable\" attribute is required");
    }


    public void setTable(boolean value) {
        isTable = value;
    }

    public boolean isTable() {
        return isTable;
    }

    public Stream setName(String name) {
        this.name = checkNotNull(name, "\"Name\" attributed is required");
        return this;
    }

    public String getName() {
        return name;
    }


    public void validate() {
        checkNotNull(name, "\"Name\" attribute is required");
    }

}
