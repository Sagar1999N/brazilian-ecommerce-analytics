package com.ecommerce.models;

public class Geolocation {
    private Integer geolocationZipCodePrefix;
    private Double geolocationLat;
    private Double geolocationLng;
    private String geolocationCity;
    private String geolocationState;

    public Geolocation() {}

    public Geolocation(Integer geolocationZipCodePrefix, Double geolocationLat, Double geolocationLng,
                       String geolocationCity, String geolocationState) {
        this.geolocationZipCodePrefix = geolocationZipCodePrefix;
        this.geolocationLat = geolocationLat;
        this.geolocationLng = geolocationLng;
        this.geolocationCity = geolocationCity;
        this.geolocationState = geolocationState;
    }

    // Getters and Setters
    public Integer getGeolocationZipCodePrefix() { return geolocationZipCodePrefix; }
    public void setGeolocationZipCodePrefix(Integer geolocationZipCodePrefix) { this.geolocationZipCodePrefix = geolocationZipCodePrefix; }

    public Double getGeolocationLat() { return geolocationLat; }
    public void setGeolocationLat(Double geolocationLat) { this.geolocationLat = geolocationLat; }

    public Double getGeolocationLng() { return geolocationLng; }
    public void setGeolocationLng(Double geolocationLng) { this.geolocationLng = geolocationLng; }

    public String getGeolocationCity() { return geolocationCity; }
    public void setGeolocationCity(String geolocationCity) { this.geolocationCity = geolocationCity; }

    public String getGeolocationState() { return geolocationState; }
    public void setGeolocationState(String geolocationState) { this.geolocationState = geolocationState; }

    @Override
    public String toString() {
        return "Geolocation{" +
                "geolocationZipCodePrefix=" + geolocationZipCodePrefix +
                ", geolocationLat=" + geolocationLat +
                ", geolocationLng=" + geolocationLng +
                ", geolocationCity='" + geolocationCity + '\'' +
                ", geolocationState='" + geolocationState + '\'' +
                '}';
    }
}