// src/main/java/com/ecommerce/models/OrderReview.java
package com.ecommerce.models;

import java.time.LocalDateTime;

public class OrderReview {
    private String reviewId;
    private String orderId;
    private Integer reviewScore;
    private String reviewCommentTitle;
    private String reviewCommentMessage;
    private LocalDateTime reviewCreationDate;
    private LocalDateTime reviewAnswerTimestamp;

    public OrderReview() {}

    public OrderReview(String reviewId, String orderId, Integer reviewScore,
                       String reviewCommentTitle, String reviewCommentMessage,
                       LocalDateTime reviewCreationDate, LocalDateTime reviewAnswerTimestamp) {
        this.reviewId = reviewId;
        this.orderId = orderId;
        this.reviewScore = reviewScore;
        this.reviewCommentTitle = reviewCommentTitle;
        this.reviewCommentMessage = reviewCommentMessage;
        this.reviewCreationDate = reviewCreationDate;
        this.reviewAnswerTimestamp = reviewAnswerTimestamp;
    }

    public String getReviewId() { return reviewId; }
    public void setReviewId(String reviewId) { this.reviewId = reviewId; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public Integer getReviewScore() { return reviewScore; }
    public void setReviewScore(Integer reviewScore) { this.reviewScore = reviewScore; }

    public String getReviewCommentTitle() { return reviewCommentTitle; }
    public void setReviewCommentTitle(String reviewCommentTitle) { this.reviewCommentTitle = reviewCommentTitle; }

    public String getReviewCommentMessage() { return reviewCommentMessage; }
    public void setReviewCommentMessage(String reviewCommentMessage) { this.reviewCommentMessage = reviewCommentMessage; }

    public LocalDateTime getReviewCreationDate() { return reviewCreationDate; }
    public void setReviewCreationDate(LocalDateTime reviewCreationDate) { this.reviewCreationDate = reviewCreationDate; }

    public LocalDateTime getReviewAnswerTimestamp() { return reviewAnswerTimestamp; }
    public void setReviewAnswerTimestamp(LocalDateTime reviewAnswerTimestamp) { this.reviewAnswerTimestamp = reviewAnswerTimestamp; }

    @Override
    public String toString() {
        return "OrderReview{" +
                "reviewId='" + reviewId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", reviewScore=" + reviewScore +
                ", reviewCommentTitle='" + reviewCommentTitle + '\'' +
                ", reviewCommentMessage='" + reviewCommentMessage + '\'' +
                ", reviewCreationDate=" + reviewCreationDate +
                ", reviewAnswerTimestamp=" + reviewAnswerTimestamp +
                '}';
    }
}