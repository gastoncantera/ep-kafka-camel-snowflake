package com.example;

import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

public class KafkaCamelService {
    private final CamelContext camelContext = new DefaultCamelContext();

    public KafkaCamelService(SnowflakeStreamingService snowflakeStreamingService) throws Exception {
        camelContext.addRoutes(new KafkaToSnowflakeRoute(snowflakeStreamingService));
    }

    public void start() {
        camelContext.start();
    }

    public void stop() {
        camelContext.stop();
    }

    private static class KafkaToSnowflakeRoute extends RouteBuilder {
        private final SnowflakeStreamingService snowflakeStreamingService;

        public KafkaToSnowflakeRoute(SnowflakeStreamingService snowflakeStreamingService) {
            this.snowflakeStreamingService = snowflakeStreamingService;
        }

        @Override
        public void configure() {
            from("kafka:snowflake-topic?brokers=localhost:9092")
                    .process(exchange ->
                            snowflakeStreamingService.sendToSnowflake(
                                    exchange.getIn().getBody(String.class),
                                    exchange.getIn().getHeaders()
                            ))
                    .log(LoggingLevel.INFO, "Message processed and sent to Snowflake");
        }
    }
}
