package com.stekytech.maxwell;

import com.stekytech.maxwell.producer.BigQuery;
import com.stekytech.maxwell.producer.BigQueryProperties;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.ProducerFactory;

import java.io.IOException;

public class BigQueryProducerFactory implements ProducerFactory {

    private final BigQueryProperties config;

    public BigQueryProducerFactory() throws IOException {
        BigQueryProperties config = new BigQueryProperties();
        this.config = config;
    }

    @Override
    public AbstractProducer createProducer(MaxwellContext context) {
        BigQuery producer = new BigQuery(context, new DatasetMap(config.projectId(), config.datasetMap()));

        return producer;
    }

}
