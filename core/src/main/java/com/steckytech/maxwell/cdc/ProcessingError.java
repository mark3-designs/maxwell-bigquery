package com.steckytech.maxwell.cdc;

import com.steckytech.maxwell.CDCEvent;

public class ProcessingError extends Processed {

    public final Throwable error;

    public ProcessingError(CDCEvent event, Throwable error) {
        super(event);
        this.error = error;
    }

}
