package com.sanjuthoas.gcp.bigtable.config;

/**
 *
 * @author Sanju Thomas
 *
 */
public class WriterConfig {

    private final String table;
    private final String keyFile;
    private final String project;
    private final String instance;

    public WriterConfig(final String keyFile, String project, String instance, final String table) {
        this.keyFile = keyFile;
        this.project = project;
        this.instance = instance;
        this.table = table;
    }

    public String table() {
        return this.table;
    }

    public String keyFile() {
        return this.keyFile;
    }

    public String project() {
        return this.project;
    }

    public String instance() {
        return this.instance;
    }
}
