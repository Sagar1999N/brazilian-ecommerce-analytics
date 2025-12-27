package com.ecommerce.util;

import net.iakovlev.timeshape.TimeZoneEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Optional;

public class TimezoneEnricher {
    private static final Logger logger = LoggerFactory.getLogger(TimezoneEnricher.class);
    
    // Initialize engine ONCE using new builder pattern
    private static final TimeZoneEngine ENGINE = TimeZoneEngine.initialize();

    public static void main(String[] args) {
        // Test São Paulo
        String tz1 = TimezoneEnricher.getTimezoneId(-23.5505, -46.6333);
        System.out.println("São Paulo: " + tz1); // → America/Sao_Paulo

        // Test Manaus (different Brazilian timezone)
        String tz2 = TimezoneEnricher.getTimezoneId(-3.1190, -60.0217);
        System.out.println("Manaus: " + tz2); // → America/Manaus

        // Test invalid
        String tz3 = TimezoneEnricher.getTimezoneId(100.0, 200.0);
        System.out.println("Invalid: " + tz3); // → invalid_coordinates
    }
    
    public static String getTimezoneId(double lat, double lon) {
        try {
            if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
                return "invalid_coordinates";
            }

            Optional<ZoneId> zoneId = ENGINE.query(lat, lon);
            return zoneId.map(ZoneId::getId).orElse("unknown");
            
        } catch (Exception e) {
            logger.warn("TimeShape error at ({}, {}): {}", lat, lon, e.getMessage());
            return "error";
        }
    }
}