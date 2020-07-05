package com.steckytech.maxwell.cdc;

import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.DDLEvent;

import java.util.function.Function;

public class MessageHandler {

    private final Function<CDCEvent, Processed> insert;
    private final Function<CDCEvent, Processed> update;
    private final Function<CDCEvent, Processed> delete;
    private final Function<DDLEvent, Processed> ddl;

    public MessageHandler(
            Function<CDCEvent, Processed> insert,
            Function<CDCEvent, Processed> update,
            Function<CDCEvent, Processed> delete,
            Function<DDLEvent, Processed> ddl
    ) {
        this.insert = insert;
        this.update = update;
        this.delete = delete;
        this.ddl = ddl;
    }

    public Processed insert(CDCEvent event) {
        return insert.apply(event);
    }
    public Processed update(CDCEvent event) {
        return update.apply(event);
    }
    public Processed delete(CDCEvent event) {
        return delete.apply(event);
    }
    public Processed ddl(DDLEvent event) {
        return ddl.apply(event);
    }
}
