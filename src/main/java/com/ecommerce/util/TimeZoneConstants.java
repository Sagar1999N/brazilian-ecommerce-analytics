package com.ecommerce.util;

import java.util.Map;

import static java.util.Map.entry;

public class TimeZoneConstants {
    public static final Map<String, String> SINGLE_TIMEZONE_COUNTRIES = Map.ofEntries(
        entry("IN", "Asia/Kolkata"),
        entry("JP", "Asia/Tokyo"),
        entry("CN", "Asia/Shanghai"),
        entry("FR", "Europe/Paris"),
        entry("DE", "Europe/Berlin"),
        entry("IT", "Europe/Rome"),
        entry("ES", "Europe/Madrid"),
        entry("SA", "Asia/Riyadh"),
        entry("EG", "Africa/Cairo"),
        entry("ZA", "Africa/Johannesburg"),
        entry("BR", "America/Sao_Paulo")
    );

    public static final java.util.Set<String> MULTI_TIMEZONE_COUNTRIES = java.util.Set.of(
        "US", "CA", "AU", "MX", "RU", "ID"
    );
}