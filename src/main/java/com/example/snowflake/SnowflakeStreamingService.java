package com.example.snowflake;

import net.snowflake.ingest.streaming.*;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
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
        props.put("private_key", generateRandomRSA2048PrivateKey());

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

    private String generateRandomRSA2048PrivateKey() throws NoSuchAlgorithmException {
        /*
         * localStack/snowflake bypass private key validation, there is no need to set the public key to the 'test' user.
         * However, SnowflakeStreamingIngestClientFactory requires a valid RSA 2048 private key.
         */
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        PrivateKey privateKey = keyPair.getPrivate();
        return Base64.getEncoder().encodeToString(privateKey.getEncoded());
    }
}