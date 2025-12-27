// src/main/java/com/ecommerce/models/OrderItem.java
package com.ecommerce.models;

import java.time.LocalDateTime;

public class OrderItem {
    private String orderId;
    private Integer orderItemId;
    private String productId;
    private String sellerId;
    private LocalDateTime shippingLimitDate;
    private Double price;
    private Double freightValue;

    // Constructors
    public OrderItem() {}

    public OrderItem(String orderId, Integer orderItemId, String productId, String sellerId,
                     LocalDateTime shippingLimitDate, Double price, Double freightValue) {
        this.orderId = orderId;
        this.orderItemId = orderItemId;
        this.productId = productId;
        this.sellerId = sellerId;
        this.shippingLimitDate = shippingLimitDate;
        this.price = price;
        this.freightValue = freightValue;
    }

    // Getters and Setters
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public Integer getOrderItemId() { return orderItemId; }
    public void setOrderItemId(Integer orderItemId) { this.orderItemId = orderItemId; }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public String getSellerId() { return sellerId; }
    public void setSellerId(String sellerId) { this.sellerId = sellerId; }

    public LocalDateTime getShippingLimitDate() { return shippingLimitDate; }
    public void setShippingLimitDate(LocalDateTime shippingLimitDate) { this.shippingLimitDate = shippingLimitDate; }

    public Double getPrice() { return price; }
    public void setPrice(Double price) { this.price = price; }

    public Double getFreightValue() { return freightValue; }
    public void setFreightValue(Double freightValue) { this.freightValue = freightValue; }

    @Override
    public String toString() {
        return "OrderItem{" +
                "orderId='" + orderId + '\'' +
                ", orderItemId=" + orderItemId +
                ", productId='" + productId + '\'' +
                ", sellerId='" + sellerId + '\'' +
                ", shippingLimitDate=" + shippingLimitDate +
                ", price=" + price +
                ", freightValue=" + freightValue +
                '}';
    }
}