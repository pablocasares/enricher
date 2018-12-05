package io.wizzie.enricher.builder;

public class KSTable<T> {
    private T table;

    KSTable() {
        this(null);
    }

    KSTable(T table) {
        this.table = table;
    }

    public void set(T table) {
        this.table = table;
    }

    public T get() {
        return table;
    }
}
