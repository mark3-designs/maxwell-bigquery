package com.steckytech.maxwell.consumer;

import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.cdc.MessageHandler;
import com.zendesk.maxwell.MaxwellContext;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Queue;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.LinkedList;

public class Collector {

    private Map<String, Queue<CDCEvent>> events = new HashMap<>();
    private List<Processor> processors = new ArrayList<>();
    private List<Thread> threads = new ArrayList<>();
    private MessageHandler handler;

    private final MaxwellContext context;

    public Collector(MaxwellContext context, MessageHandler handler) {
        this.context = context;
        this.handler = handler;
    }

    @PreDestroy
    public void stop() {
        for (Processor proc : processors) {
            proc.shutdown();
        }

        for (Thread thread : threads) {
            while (thread.isAlive()) {
                // wait
                try {
                    Thread.sleep(60);
                } catch (InterruptedException interrupted) {

                }
            }
        }
    }

    synchronized
    public void collect(CDCEvent event) {
        Queue q = events.get(event.partition);
        if (q == null) {
            q = new LinkedList();
            Processor proc = new Processor(context, event.partition, q, handler);
            Thread thread = new Thread(proc);
            processors.add(proc);
            threads.add(thread);
            events.put(event.partition, q);
            thread.start();
        }
        q.add(event);
    }



}
