package com.ecommerce.models;

import java.time.LocalDateTime;

public class Order {
	private String orderId;
	private String customerId;
	private String orderStatus;
	private LocalDateTime purchaseTimestamp;
	private LocalDateTime approvedAt;
	private LocalDateTime deliveredCarrierDate;
	private LocalDateTime deliveredCustomerDate;
	private LocalDateTime estimatedDeliveryDate;

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getOrderStatus() {
		return orderStatus;
	}

	public void setOrderStatus(String orderStatus) {
		this.orderStatus = orderStatus;
	}

	public LocalDateTime getPurchaseTimestamp() {
		return purchaseTimestamp;
	}

	public void setPurchaseTimestamp(LocalDateTime purchaseTimestamp) {
		this.purchaseTimestamp = purchaseTimestamp;
	}

	public LocalDateTime getApprovedAt() {
		return approvedAt;
	}

	public void setApprovedAt(LocalDateTime approvedAt) {
		this.approvedAt = approvedAt;
	}

	public LocalDateTime getDeliveredCarrierDate() {
		return deliveredCarrierDate;
	}

	public void setDeliveredCarrierDate(LocalDateTime deliveredCarrierDate) {
		this.deliveredCarrierDate = deliveredCarrierDate;
	}

	public LocalDateTime getDeliveredCustomerDate() {
		return deliveredCustomerDate;
	}

	public void setDeliveredCustomerDate(LocalDateTime deliveredCustomerDate) {
		this.deliveredCustomerDate = deliveredCustomerDate;
	}

	public LocalDateTime getEstimatedDeliveryDate() {
		return estimatedDeliveryDate;
	}

	public void setEstimatedDeliveryDate(LocalDateTime estimatedDeliveryDate) {
		this.estimatedDeliveryDate = estimatedDeliveryDate;
	}

	@Override
	public String toString() {
		return "Order [orderId=" + orderId + ", customerId=" + customerId + ", orderStatus=" + orderStatus
				+ ", purchaseTimestamp=" + purchaseTimestamp + ", approvedAt=" + approvedAt + ", deliveredCarrierDate="
				+ deliveredCarrierDate + ", deliveredCustomerDate=" + deliveredCustomerDate + ", estimatedDeliveryDate="
				+ estimatedDeliveryDate + "]";
	}

	public Order(String orderId, String customerId, String orderStatus, LocalDateTime purchaseTimestamp,
			LocalDateTime approvedAt, LocalDateTime deliveredCarrierDate, LocalDateTime deliveredCustomerDate,
			LocalDateTime estimatedDeliveryDate) {
		super();
		this.orderId = orderId;
		this.customerId = customerId;
		this.orderStatus = orderStatus;
		this.purchaseTimestamp = purchaseTimestamp;
		this.approvedAt = approvedAt;
		this.deliveredCarrierDate = deliveredCarrierDate;
		this.deliveredCustomerDate = deliveredCustomerDate;
		this.estimatedDeliveryDate = estimatedDeliveryDate;
	}

	public Order() {
		super();
		// TODO Auto-generated constructor stub
	}

	// Constructors, getters, setters, toString()

}