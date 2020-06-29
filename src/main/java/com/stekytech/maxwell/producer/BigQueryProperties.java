package com.stekytech.maxwell.producer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class BigQueryProperties {

    private Properties properties = new Properties();

    public BigQueryProperties() throws IOException {
        this("/bigquery.properties");
    }

    public BigQueryProperties(String resource) throws IOException {
        Properties properties = new Properties();
        properties.load( getClass().getResourceAsStream(resource));
    }

    public String projectId() {
        return properties.getProperty("project");
    }

    public Map<String, String> datasetMap() {

        return properties.keySet().stream()
                .map(k -> (String)k)
                .filter(k -> k.startsWith("map."))
                .collect(Collectors.toMap(k -> k, v -> properties.getProperty(v)));


    }


}
