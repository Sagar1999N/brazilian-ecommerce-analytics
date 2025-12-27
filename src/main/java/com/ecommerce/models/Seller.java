// src/main/java/com/ecommerce/models/Seller.java
package com.ecommerce.models;

public class Seller {
    private String sellerId;
    private Integer sellerZipCodePrefix;
    private String sellerCity;
    private String sellerState;

    public Seller() {}

    public Seller(String sellerId, Integer sellerZipCodePrefix, String sellerCity, String sellerState) {
        this.sellerId = sellerId;
        this.sellerZipCodePrefix = sellerZipCodePrefix;
        this.sellerCity = sellerCity;
        this.sellerState = sellerState;
    }

    public String getSellerId() { return sellerId; }
    public void setSellerId(String sellerId) { this.sellerId = sellerId; }

    public Integer getSellerZipCodePrefix() { return sellerZipCodePrefix; }
    public void setSellerZipCodePrefix(Integer sellerZipCodePrefix) { this.sellerZipCodePrefix = sellerZipCodePrefix; }

    public String getSellerCity() { return sellerCity; }
    public void setSellerCity(String sellerCity) { this.sellerCity = sellerCity; }

    public String getSellerState() { return sellerState; }
    public void setSellerState(String sellerState) { this.sellerState = sellerState; }

    @Override
    public String toString() {
        return "Seller{" +
                "sellerId='" + sellerId + '\'' +
                ", sellerZipCodePrefix=" + sellerZipCodePrefix +
                ", sellerCity='" + sellerCity + '\'' +
                ", sellerState='" + sellerState + '\'' +
                '}';
    }
}