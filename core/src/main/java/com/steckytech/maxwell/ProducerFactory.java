package com.steckytech.maxwell;

import com.steckytech.maxwell.cdc.MessageHandler;
import com.steckytech.maxwell.producer.CDCStream;
import com.steckytech.maxwell.producer.Settings;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;

import java.io.IOException;

public class ProducerFactory implements com.zendesk.maxwell.producer.ProducerFactory {

    private final Settings config;

    public ProducerFactory() throws IOException {
        Settings config = new Settings();
        this.config = config;
    }

    @Override
    public AbstractProducer createProducer(MaxwellContext context) {

        MessageHandler handler = new MessageHandler(
               config.insertHandler(),
               config.updateHandler(),
               config.deleteHandler(),
               config.ddlHandler()
        );

        CDCStream producer = new CDCStream(context, new DatasetMap(config.projectId(), config.datasetMap()), handler);

        return producer;
    }

}
