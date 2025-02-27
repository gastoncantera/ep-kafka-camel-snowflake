package com.example.snowflake;

import net.snowflake.ingest.streaming.*;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SnowflakeStreamingService {
    private static final Logger logger = Logger.getLogger(SnowflakeStreamingService.class.getName());

    private SnowflakeStreamingIngestClient client;
    private SnowflakeStreamingIngestChannel channel;

    public SnowflakeStreamingService() throws Exception {
        Properties props = new Properties();
        props.put("scheme", "http");
        props.put("host", "snowflake.localhost.localstack.cloud");
        props.put("port", 4566);
        props.put("user", "test");
        props.put("role", "test");
        props.put("private_key", "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC7Cipynz7AUQ/itdZG1v+Rl6t2kl4OobKefKH5EaBOu0Rey1l+y4rXVNn1gLieEj0WWoXB76ugODbrOFoQ2UedTTBg/fdiHrT4PraS5UirpD7Uw8mQi80m3Ehqq82w22WZmh3ds8vNUD5jR0pM/UPYaQcunEDKIDgBMrmduqtNC4NXoSOl/kpGIjUQ6Mf1VruaywvCb8/PIjEeMtDU/ZHX6hrDw+X0A6ScePV4shbmVZeqHIlGawgFlR3QZl64S+mjEZCIjpPfSU05uZ8qNGmt3UgWDYPWmJrTrO+Ag6IMmVogSAllPN8iQCc23GSReNHRYUZZiMfTNeL67EeIIh+TAgMBAAECggEAUOje3+KfRJZMZuf+H3rV52dS1uIOKgosuH51msbTL/u2YcNZnY3zK58bAiaCtm5xWoAiKHjDJ3Xp2+rumydC4XsImIyEPT/HTTPjyrvAqe9M9sETKqIvRsY31V0oh38A/mc60DL80H4L3upx68gLV0xnvPP+2vEBCROmU+LyQ0sf1uDF4MVrCoMHZqJprgVxLgKRvDorLZs0bcQ7kNueWwilvm/64ursQFC1ceaibOHV62udNcr+z4O6aaeiU85wv6knbos9CzQ/sJV1zihXuYXT0ztqQkNnPeoRt0MIPOxXaMtyjEdDiHuixd9wqpJbm9FCLlBRHtDgsVfjLFhs4QKBgQDr1Yhh6vQrk1MqTTP+IfO8Fw03rWiE2EOk8qqwGZgO6A2iSWuMctC/35xid42Z9rwemZPz8H26jdlroAxcciFXWNlOoZmHVZxHtRq5TRUF4uNj3jEU8VPOOi/9nupJEmZmoDBSZBDxeY7AD/fmzHpZAsZkRDwoCeSxJKaP6ukw6QKBgQDLCILxSWBgQbwUcpm/8WkJj5dTr8kCZsPx5J9SqHDg18c5npNuDH89kENNLdu4s2KeGB93+bqe8x0vNJ0C8oYO5xuIP5uHikvZ0s9ZajSqX1fBR9na7uXTim6pql1pO1FqUx2XHPezXRn6Ev0Q52g2HCorwO/NEhAOi7vOgxffGwKBgG/KZr3TvC6zxWZ19kFvIrR0UOWlo7flNBuKlHKpjzTxtxTIrNyEyINLojvi5BKprP7sWf/2bgLynq+vzGw+BaP8D/aAD+DhKhWruaFA8sg5hwSeHLIKu2k0l+8nV3OP706SfJVxrb5pmstcRmz3XL+42wZIeiOYnPmDoBj0h9mRAoGADly/xonGQ4ji4R9qOOW+5Go/7i+VLJJQciAWFSbNNVqOQUkybKp9pcE6wY7o6Bvocf6K21XTGcNg4SH7qWW2jf9TN0QooGHsE7CR8mVM7HBqKMYIZzBXGavFQENI8FS94aOXiEUIUKvZVpNZS4TTHmDHquivDfalJCJGWslzOtkCgYBnn5chqkuoUjK94+2wZ2sQNb0WuDHRaWNdVoOWsi0Z+4ACk8N6vG93lpgRhvJF1LUxowRmig2jK8Kl+VwUHFu4LmzCQ/f5iab8G6PhGm9hS5z3FnFHaI3AhD6sXs3J2sGJcMsTOy99BFY09We3S1dHbz/8T61CEbmPzfuo4h5uKA==");

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

    public void sendToSnowflake(String content, String metadata) {
        try {
            InsertValidationResponse response = channel.insertRow(
                    Map.of(
                            "RECORD_CONTENT", content,
                            "RECORD_METADATA", metadata
                    ),
                    UUID.randomUUID().toString()
            );

            if (response.hasErrors()) {
                throw response.getInsertErrors().get(0).getException();
            }

            logger.log(Level.INFO, "✅ Data sent to Snowflake");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "❌ Error sending data to Snowflake: " + e.getMessage());
        }
    }
}