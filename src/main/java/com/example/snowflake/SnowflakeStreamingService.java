package com.example.snowflake;

import net.snowflake.ingest.streaming.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SnowflakeStreamingService {
    private static final Logger logger = Logger.getLogger(SnowflakeStreamingService.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();

    private SnowflakeStreamingIngestClient client;
    private SnowflakeStreamingIngestChannel channel;

    public SnowflakeStreamingService() throws Exception {
        String PROFILE_PATH = "src/main/resources/profile.json";

        Properties props = new Properties();
        Iterator<Map.Entry<String, JsonNode>> propIt =
                mapper.readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH)))).fields();
        while (propIt.hasNext()) {
            Map.Entry<String, JsonNode> prop = propIt.next();
            props.put(prop.getKey(), prop.getValue().asText());
        }

        this.client = SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT")
                .setProperties(props)
                .build();
    }

    public void start() {
        OpenChannelRequest request = OpenChannelRequest.builder("MY_CHANNEL")
                .setDBName("test")
                .setSchemaName("public")
                .setTableName("\"kafka-streaming\"")
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                .build();

        this.channel = client.openChannel(request);
    }

    public void stop() throws Exception {
        channel.close().get();
        client.close();
    }

    public void sendToSnowflake(String message, Map<String, Object> metadata) {
        try {
            Map<String, Object> row = Map.of(
                    "RECORD_CONTENT", message,
                    "RECORD_METADATA", mapper.writeValueAsString(metadata)
            );

            InsertValidationResponse response = channel.insertRow(row, UUID.randomUUID().toString());

            if (response.hasErrors()) {
                throw response.getInsertErrors().get(0).getException();
            }

            logger.log(Level.INFO, "✅ Data sent to Snowflake: " + row);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "❌ Error sending data to Snowflake: " + e.getMessage());
        }
    }
}