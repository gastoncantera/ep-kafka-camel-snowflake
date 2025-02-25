package com.example;

import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

public class CamelService {
    private final CamelContext camelContext = new DefaultCamelContext();

    public CamelService(SnowpipeService snowpipeService) throws Exception {
        camelContext.addRoutes(new KafkaToSnowflakeRoute(snowpipeService));
    }

    public void start() {
        camelContext.start();
    }

    public void stop() {
        camelContext.stop();
    }

    private static class KafkaToSnowflakeRoute extends RouteBuilder {
        private final SnowpipeService snowpipeService;

        public KafkaToSnowflakeRoute(SnowpipeService snowpipeService) {
            this.snowpipeService = snowpipeService;
        }

        @Override
        public void configure() {
            from("kafka:snowflake-topic?brokers=localhost:9092")
                    .unmarshal().json()
                    .log(LoggingLevel.INFO, "Message received: ${body}")
                    .bean(snowpipeService, "sendToSnowflake");
        }
    }
}
