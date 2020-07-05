package com.steckytech.maxwell.cdc;

import com.steckytech.maxwell.CDCEvent;
import com.zendesk.maxwell.MaxwellContext;

public class Processed {

    public final CDCEvent event;

    public Processed(CDCEvent event) {
        this.event = event;
    }

    public void commit(MaxwellContext maxwell) {
        event.commit(maxwell);
    }

}
