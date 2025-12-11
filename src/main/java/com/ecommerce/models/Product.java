package com.ecommerce.models;

public class Product {
    private String productId;
    private String categoryName;
    private Double productWeightG;
    private Double productLengthCm;
    private Double productHeightCm;
    private Double productWidthCm;
	public String getProductId() {
		return productId;
	}
	public void setProductId(String productId) {
		this.productId = productId;
	}
	public String getCategoryName() {
		return categoryName;
	}
	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}
	public Double getProductWeightG() {
		return productWeightG;
	}
	public void setProductWeightG(Double productWeightG) {
		this.productWeightG = productWeightG;
	}
	public Double getProductLengthCm() {
		return productLengthCm;
	}
	public void setProductLengthCm(Double productLengthCm) {
		this.productLengthCm = productLengthCm;
	}
	public Double getProductHeightCm() {
		return productHeightCm;
	}
	public void setProductHeightCm(Double productHeightCm) {
		this.productHeightCm = productHeightCm;
	}
	public Double getProductWidthCm() {
		return productWidthCm;
	}
	public void setProductWidthCm(Double productWidthCm) {
		this.productWidthCm = productWidthCm;
	}
	@Override
	public String toString() {
		return "Product [productId=" + productId + ", categoryName=" + categoryName + ", productWeightG="
				+ productWeightG + ", productLengthCm=" + productLengthCm + ", productHeightCm=" + productHeightCm
				+ ", productWidthCm=" + productWidthCm + "]";
	}
	public Product(String productId, String categoryName, Double productWeightG, Double productLengthCm,
			Double productHeightCm, Double productWidthCm) {
		super();
		this.productId = productId;
		this.categoryName = categoryName;
		this.productWeightG = productWeightG;
		this.productLengthCm = productLengthCm;
		this.productHeightCm = productHeightCm;
		this.productWidthCm = productWidthCm;
	}
	public Product() {
		super();
		// TODO Auto-generated constructor stub
	}
    
    
    // Constructors, getters, setters
}