package com.example;

import com.example.snowflake.SnowflakeStreamingService;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        SnowflakeStreamingService snowflakeStreamingService = new SnowflakeStreamingService();
        KafkaCamelService kafkaCamelService = new KafkaCamelService(snowflakeStreamingService);

        try {
            snowflakeStreamingService.start();
            kafkaCamelService.start();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "❌ Error starting services: " + e.getMessage());
            throw e;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.log(Level.INFO, "🛑 Stopping services...");
            try {
                kafkaCamelService.stop();
                snowflakeStreamingService.stop();
            } catch (Exception e) {
                logger.log(Level.WARNING, "⚠️ Error stopping services: " + e.getMessage());
            }
        }));

        synchronized (Main.class) {
            Main.class.wait();
        }
    }
}