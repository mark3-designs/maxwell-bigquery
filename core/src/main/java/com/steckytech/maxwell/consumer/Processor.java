package com.steckytech.maxwell.consumer;

import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.DDLEvent;
import com.steckytech.maxwell.cdc.MessageHandler;
import com.steckytech.maxwell.cdc.MessageType;
import com.steckytech.maxwell.cdc.Processed;
import com.zendesk.maxwell.MaxwellContext;
import com.steckytech.maxwell.cdc.ProcessingError;
import com.zendesk.maxwell.bootstrap.BootstrapController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Queue;

public class Processor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);

    public final String partition;

    private final Queue<CDCEvent> queue;
    private final MessageHandler handlers;
    private final MaxwellContext maxwell;

    private int backoff = 10;

    private boolean shutdown = false;



    public Processor(MaxwellContext context, String partition, Queue<CDCEvent> queue, MessageHandler handlers) {
        this.partition = partition;
        this.queue = queue;
        this.handlers = handlers;
        this.maxwell = context;
    }

    @Override
    public void run() {

        while (!shutdown) {

            while (queue.peek() == null) {
                try {
                    // maxwell.heartbeat();
                    Thread.sleep(backoff());
                } catch (InterruptedException irq) {
                    // interrupted
                } catch (Throwable error) {
                    error.printStackTrace();
                }
            }

            process(Optional.ofNullable(queue.poll()))
                    .ifPresent(processed -> processed.commit(maxwell));

        }

    }

    private Optional<Processed> process(Optional<CDCEvent> message) {
        if (!message.isPresent()) { return Optional.empty(); }


        CDCEvent event = message.get();

        try {
            BootstrapController boot = maxwell.getBootstrapController(event.schema_id);
        } catch (IOException iox) {
            LOG.error("Failed to get bootstrap information for schema_id="+ event.schema_id +" [" + iox.getMessage() +"]", iox);
        }

        try {
            switch (event.type) {
                case ddl:
                    return Optional.of(handlers.ddl((DDLEvent)event));
                case update:
                    return Optional.of(handlers.update(event));
                case insert:
                    return Optional.of(handlers.insert(event));
                case delete:
                    return Optional.of(handlers.delete(event));
                default:
                    return Optional.of(new ProcessingError(event, new Exception("Unhandled type: "+ event.type)));
            }
        } catch (Throwable error) {
            error.printStackTrace();
            return Optional.of(new ProcessingError(event, error));
        }
    }


    public void shutdown() {
        shutdown = true;
        queue.clear();
    }


    private int backoff() {
        backoff = (int)(backoff * 1.25);
        if (backoff > 1800) {
            backoff = 10;
        }
        return backoff;
    }
}
