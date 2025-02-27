package com.example;

import com.example.snowflake.SnowflakeStreamingService;
import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

import java.util.HashMap;
import java.util.Map;

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
            from("kafka:snowflake-topic?brokers=localhost:9092&groupId=ep-kafka-camel-snowflake")

                    // Snowpipe Streaming: Snowflake Ingest Service Java SDK
                    // https://github.com/snowflakedb/snowflake-ingest-java
                    .process(exchange ->
                            snowflakeStreamingService.sendToSnowflake(
                                    exchange.getIn().getBody(String.class),
                                    exchange.getIn().getHeaders()
                            ))

                    // Apache Camel Kafka Connector: Kamelet Snowflake Sink
                    // https://github.com/apache/camel-kafka-connector/tree/main/connectors/camel-snowflake-sink-kafka-connector
                    // https://camel.apache.org/camel-kafka-connector/4.8.x/reference/connectors/camel-snowflake-sink-kafka-sink-connector.html
                    .toD("kamelet:snowflake-sink"
                            + "?instanceUrl=http://snowflake.localhost.localstack.cloud:4566"
                            + "&username=test"
                            + "&password=test"
                            + "&databaseName=test"
                            + "&query=INSERT INTO public.\"kafka-streaming\" (RECORD_CONTENT,RECORD_METADATA) VALUES ('${body}','${headers}')"
                    )

                    .log(LoggingLevel.INFO, "Message processed and sent to Snowflake");
        }
    }
}
