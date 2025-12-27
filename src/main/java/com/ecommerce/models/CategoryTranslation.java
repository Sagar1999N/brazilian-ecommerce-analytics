// src/main/java/com/ecommerce/models/CategoryTranslation.java
package com.ecommerce.models;

public class CategoryTranslation {
    private String productCategoryName;
    private String productCategoryNameEnglish;

    public CategoryTranslation() {}

    public CategoryTranslation(String productCategoryName, String productCategoryNameEnglish) {
        this.productCategoryName = productCategoryName;
        this.productCategoryNameEnglish = productCategoryNameEnglish;
    }

    public String getProductCategoryName() { return productCategoryName; }
    public void setProductCategoryName(String productCategoryName) { this.productCategoryName = productCategoryName; }

    public String getProductCategoryNameEnglish() { return productCategoryNameEnglish; }
    public void setProductCategoryNameEnglish(String productCategoryNameEnglish) { this.productCategoryNameEnglish = productCategoryNameEnglish; }

    @Override
    public String toString() {
        return "CategoryTranslation{" +
                "productCategoryName='" + productCategoryName + '\'' +
                ", productCategoryNameEnglish='" + productCategoryNameEnglish + '\'' +
                '}';
    }
}