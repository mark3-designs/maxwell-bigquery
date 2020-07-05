package com.steckytech.maxwell;

import java.util.HashMap;
import java.util.Map;

public class DatasetMap {

    public final String project;

    private final Map<String, String> datasetMap = new HashMap<>(); // map database to dataset

    public DatasetMap(String project) {
        this.project = project;
    }

    public DatasetMap(String project, Map<String, String> datasetMap) {
        this(project);
        this.datasetMap.putAll(datasetMap);
    }

    public DatasetMap add(String database, String dataset) {
        datasetMap.put(database, dataset);
        return this;
    }

    public String datasetFor(String database) {
        return datasetMap.getOrDefault(database, database);
    }
}
