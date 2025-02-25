package com.example;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        SnowpipeService snowpipeService = new SnowpipeService();
        CamelService camelService = new CamelService(snowpipeService);

        try {
            snowpipeService.start();
            camelService.start();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "❌ Error starting services: " + e.getMessage());
            throw e;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.log(Level.INFO, "🛑 Stopping services...");
            try {
                camelService.stop();
                snowpipeService.stop();
            } catch (Exception e) {
                logger.log(Level.WARNING, "⚠️ Error stopping services: " + e.getMessage());
            }
        }));

        synchronized (Main.class) {
            Main.class.wait();
        }
    }
}