package com.steckytech.maxwell.consumer;

import com.steckytech.maxwell.cdc.Processed;
import com.steckytech.maxwell.cdc.MessageType;

import java.util.function.Function;

public abstract class Handler<T> implements Function<T, Processed> {

    public final MessageType type;

    public Handler(MessageType type) {
        this.type = type;
    }

    public abstract Processed apply(T event);

}
