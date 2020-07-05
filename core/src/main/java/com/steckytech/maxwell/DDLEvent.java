package com.steckytech.maxwell;

import com.steckytech.maxwell.cdc.DDL;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractAsyncProducer;
import com.zendesk.maxwell.schema.ddl.DDLMap;

import java.util.Map;

public class DDLEvent extends CDCEvent {

    public final DDL changeType;

    private final DDLMap ddl;

    public DDLEvent(MaxwellContext context, String dataset, DDLMap data, AbstractAsyncProducer.CallbackCompleter completer) {
        super(context, dataset, data, completer);
        this.ddl = data;
        this.changeType = DDL.valueOf(((String)(data.getChangeMap().get("type"))).replaceAll("-", "_"));
    }

    public Map<String, Object> getChangeMap() {
        return ddl.getChangeMap();
    }

    public String getSql() {
        return ddl.getSql();
    }

    public String getTable() {
        return ddl.getTable();
    }

    public String getDatabase() {
        return ddl.getDatabase();
    }


}
