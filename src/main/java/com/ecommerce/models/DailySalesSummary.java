package com.ecommerce.models;

import java.time.LocalDate;

public class DailySalesSummary {
    private LocalDate saleDate;
    private String category;
    private Long orders;
    private Double totalRevenue;
    private Double avgOrderValue;
    private Long itemsSold;

    // Getters and Setters
    public LocalDate getSaleDate() { return saleDate; }
    public void setSaleDate(LocalDate saleDate) { this.saleDate = saleDate; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public Long getOrders() { return orders; }
    public void setOrders(Long orders) { this.orders = orders; }

    public Double getTotalRevenue() { return totalRevenue; }
    public void setTotalRevenue(Double totalRevenue) { this.totalRevenue = totalRevenue; }

    public Double getAvgOrderValue() { return avgOrderValue; }
    public void setAvgOrderValue(Double avgOrderValue) { this.avgOrderValue = avgOrderValue; }

    public Long getItemsSold() { return itemsSold; }
    public void setItemsSold(Long itemsSold) { this.itemsSold = itemsSold; }
}