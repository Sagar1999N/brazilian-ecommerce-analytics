// src/main/java/com/ecommerce/models/OrderPayment.java
package com.ecommerce.models;

public class OrderPayment {
    private String orderId;
    private Integer paymentSequential;
    private String paymentType;
    private Integer paymentInstallments;
    private Double paymentValue;

    public OrderPayment() {}

    public OrderPayment(String orderId, Integer paymentSequential, String paymentType,
                        Integer paymentInstallments, Double paymentValue) {
        this.orderId = orderId;
        this.paymentSequential = paymentSequential;
        this.paymentType = paymentType;
        this.paymentInstallments = paymentInstallments;
        this.paymentValue = paymentValue;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public Integer getPaymentSequential() { return paymentSequential; }
    public void setPaymentSequential(Integer paymentSequential) { this.paymentSequential = paymentSequential; }

    public String getPaymentType() { return paymentType; }
    public void setPaymentType(String paymentType) { this.paymentType = paymentType; }

    public Integer getPaymentInstallments() { return paymentInstallments; }
    public void setPaymentInstallments(Integer paymentInstallments) { this.paymentInstallments = paymentInstallments; }

    public Double getPaymentValue() { return paymentValue; }
    public void setPaymentValue(Double paymentValue) { this.paymentValue = paymentValue; }

    @Override
    public String toString() {
        return "OrderPayment{" +
                "orderId='" + orderId + '\'' +
                ", paymentSequential=" + paymentSequential +
                ", paymentType='" + paymentType + '\'' +
                ", paymentInstallments=" + paymentInstallments +
                ", paymentValue=" + paymentValue +
                '}';
    }
}