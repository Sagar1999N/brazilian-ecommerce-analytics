package com.ecommerce.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Centralized configuration management using Typesafe Config.
 * Supports environment-specific configurations and system property overrides.
 */
public class AppConfig {
    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);
    private static Config config;
    
    static {
        loadConfig();
    }
    
    private static void loadConfig() {
        try {
            // Determine environment
            String env = System.getProperty("env", 
                         System.getenv().getOrDefault("ENV", "dev"));
            
            logger.info("Loading configuration for environment: {}", env);
            
            // Load base configuration
            Config baseConfig = ConfigFactory.load("application.conf");
            
            // Load environment-specific configuration
            Config envConfig = ConfigFactory.load("application-" + env + ".conf");
            
            // System properties override
            Config systemConfig = ConfigFactory.systemProperties();
            
            // Combine with environment-specific having highest priority
            config = envConfig
                .withFallback(baseConfig)
                .withFallback(systemConfig)
                .resolve();
            
            logger.info("Configuration loaded successfully for environment: {}", env);
            
        } catch (Exception e) {
            logger.error("Failed to load configuration", e);
            // Fallback to default
            config = ConfigFactory.load();
        }
    }
    
    public static Config getConfig() {
        return config;
    }
    
    public static String getString(String path) {
        return config.getString(path);
    }
    
    public static String getString(String path, String defaultValue) {
        try {
            return config.getString(path);
        } catch (Exception e) {
            return defaultValue;
        }
    }
    
    public static int getInt(String path) {
        return config.getInt(path);
    }
    
    public static int getInt(String path, int defaultValue) {
        try {
            return config.getInt(path);
        } catch (Exception e) {
            return defaultValue;
        }
    }
    
    public static boolean getBoolean(String path) {
        return config.getBoolean(path);
    }
    
    public static boolean getBoolean(String path, boolean defaultValue) {
        try {
            return config.getBoolean(path);
        } catch (Exception e) {
            return defaultValue;
        }
    }
    
    public static Config getSubConfig(String path) {
        return config.getConfig(path);
    }
    
    public static Map<String, String> getSparkConfigs() {
        Map<String, String> sparkConfigs = new HashMap<>();
        Config sparkConfig = config.getConfig("app.spark.config");
        
        sparkConfig.entrySet().forEach(entry -> {
            sparkConfigs.put(entry.getKey(), entry.getValue().unwrapped().toString());
        });
        
        return sparkConfigs;
    }
    
    public static String getEnvironment() {
        return getString("app.environment", "dev");
    }
    
    public static boolean isDevelopment() {
        return "development".equalsIgnoreCase(getEnvironment()) || 
               "dev".equalsIgnoreCase(getEnvironment());
    }
    
    public static boolean isProduction() {
        return "production".equalsIgnoreCase(getEnvironment()) || 
               "prod".equalsIgnoreCase(getEnvironment());
    }
}