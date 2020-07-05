package com.steckytech.maxwell;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.steckytech.maxwell.cdc.MessageType;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractAsyncProducer;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CDCEvent {

    private static final Logger LOG = LoggerFactory.getLogger(CDCEvent.class);
    private final boolean DEBUG = LOG.isDebugEnabled();

    public final MessageType type;

    public final String partition;
    public final String dataset;
    public final String database;
    public final String table;
    public final Long schema_id;
    public final Map<String, Object> row;
    public final Map<String, Object> old;
    public final Position position;
    public final Position nextPosition;

    private final MaxwellContext context;
    private final AbstractAsyncProducer.CallbackCompleter completer;
    private final RowMap data;

    public CDCEvent(MaxwellContext context, String dataset, RowMap data, AbstractAsyncProducer.CallbackCompleter completer) {
        this.type = MessageType.valueOf(data.getRowType());

        this.database = data.getDatabase();
        this.table = data.getTable();

        this.position = data.getPosition();
        this.nextPosition = data.getNextPosition();
        this.dataset = dataset;
        this.data = data;
        this.completer = completer;

        this.context = context;
        this.schema_id = data == null ? null : data.getSchemaId();

        this.row = Collections.unmodifiableMap(data.getData());
        this.old = Collections.unmodifiableMap(data.getOldData());

        this.partition = data.getPartitionString(); // data.getDatabase() +"."+ data.getTable();

    }

    public Object getPrimaryKey() {
        try {
            return data.pkToJson(RowMap.KeyFormat.ARRAY);
        } catch (Exception x) {
            return null;
        }
    }

    public String toJSON() {
        try {
            return data.toJSON(context.getConfig().outputConfig);
        } catch (Exception x) {
            LOG.error("JSON Encode Failed: "+ x.getMessage(), x);
            return null;
        }
    }

    public void commit(MaxwellContext maxwell) {
        completer.markCompleted();
        maxwell.setPosition(nextPosition);
        if (DEBUG) {
            LOG.debug("Commit " + nextPosition);
        }
    }


    @Override
    public String toString() {

        ObjectWriter json = new ObjectMapper().writer().withDefaultPrettyPrinter();

        Map<String, Object> obj = new HashMap<>();

        obj.put("partition", data.getPartitionString());
        obj.put("database", data.getDatabase());
        obj.put("table", data.getTable());
        obj.put("server", data.getServerId());

        Map<String, Object> position = new HashMap<>();
        position.put("current", data.getPosition().toCommandline());
        position.put("next", data.getNextPosition().toCommandline());

        obj.put("position", position);

        obj.put("type", type);

        if (data.getData().size() > 0) {
            obj.put("columns", data.getData().keySet());
            obj.put("row_values", data.getData());
        }
        if (data.getOldData().size() > 0) {
            obj.put("changed_columns", data.getOldData().keySet());
            obj.put("old_values", data.getOldData());
        }
        if (data.getExtraAttributes().size() > 0) {
            obj.put("extra", data.getExtraAttributes());
        }

        if (this instanceof DDLEvent) {
            DDLEvent ddl = (DDLEvent) this;
            obj.put("ddl", ddl.changeType);
            obj.put("sql", ddl.getSql());
            obj.put("change", ddl.getChangeMap());
            obj.put("fields", ddl.getChangeMap().keySet());
        }

        try {
            return json.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            LOG.error(e.getMessage(), e);
            return "{ \"error\": \""+ e.getMessage() +"\" }";
        }
    }

}
