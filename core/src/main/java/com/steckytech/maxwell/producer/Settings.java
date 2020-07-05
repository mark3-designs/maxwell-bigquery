package com.steckytech.maxwell.producer;

import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.DDLEvent;
import com.steckytech.maxwell.Util;
import com.steckytech.maxwell.cdc.Processed;
import com.steckytech.maxwell.handler.stdout.DeleteRow;
import com.steckytech.maxwell.handler.stdout.InsertRow;
import com.steckytech.maxwell.handler.stdout.SchemaChange;
import com.steckytech.maxwell.handler.stdout.UpdateRow;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Settings {

    private Properties properties = new Properties();

    public Settings() throws IOException {
        this("/stream.properties");
    }

    public Settings(String resource) throws IOException {
        this.properties.load( getClass().getResourceAsStream(resource));
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

    public Function<CDCEvent, Processed> insertHandler() {
        try {
            return (Function<CDCEvent, Processed>) Util.instantiateObject(properties.getProperty("insert.function", InsertRow.class.getCanonicalName()));
        } catch (Throwable x) {
            throw new Error("Failed to instantiate insert.function '"+ properties.getProperty("insert.function") +"' ["+ x.getMessage() +"]", x);
        }
    }
    public Function<CDCEvent, Processed> updateHandler() {
        try {
            return (Function<CDCEvent, Processed>) Util.instantiateObject(properties.getProperty("update.function", UpdateRow.class.getCanonicalName()));
        } catch (Throwable x) {
            throw new Error("Failed to instantiate update.function '"+ properties.getProperty("update.function") +"' ["+ x.getMessage() +"]", x);
        }
    }
    public Function<CDCEvent, Processed> deleteHandler() {
        try {
            return (Function<CDCEvent, Processed>) Util.instantiateObject(properties.getProperty("delete.function", DeleteRow.class.getCanonicalName()));
        } catch (Throwable x) {
            throw new Error("Failed to instantiate delete.function '"+ properties.getProperty("delete.function") +"' ["+ x.getMessage() +"]", x);
        }
    }
    public Function<DDLEvent, Processed> ddlHandler() {
        try {
            return (Function<DDLEvent, Processed>) Util.instantiateObject(properties.getProperty("ddl.function", SchemaChange.class.getCanonicalName()));
        } catch (Throwable x) {
            throw new Error("Failed to instantiate ddl.function '"+ properties.getProperty("ddl.function") +"' ["+ x.getMessage() +"]", x);
        }
    }


}
