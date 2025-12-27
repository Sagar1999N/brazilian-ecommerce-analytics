package com.ecommerce.ingestion;

import com.ecommerce.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Downloads and extracts the Brazilian e-commerce dataset.
 * For Kaggle datasets, you need to set up Kaggle API credentials.
 */
public class DatasetDownloader {
    private static final Logger logger = LoggerFactory.getLogger(DatasetDownloader.class);
    
    // Kaggle dataset URL (you'll need Kaggle API for actual download)
    private static final String KAGGLE_DATASET = "olistbr/brazilian-ecommerce";
    private static final String KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/" + KAGGLE_DATASET + "/download";
    
    public static void downloadDataset() {
        String rawDataPath = AppConfig.getString("app.data.raw.path");
        String zipFilePath = rawDataPath + "brazilian-ecommerce.zip";
        
        logger.info("Starting dataset download to: {}", rawDataPath);
        
        try {
            // Create directory if it doesn't exist
            Files.createDirectories(Paths.get(rawDataPath));
            
            // For now, we'll create sample data since Kaggle requires API
            // In production, you would use Kaggle API with credentials
            createSampleData(rawDataPath);
            
            logger.info("Dataset preparation completed at: {}", rawDataPath);
            
        } catch (Exception e) {
            logger.error("Failed to download dataset", e);
            throw new RuntimeException("Dataset download failed", e);
        }
    }
    
    private static void createSampleData(String rawDataPath) throws IOException {
        logger.info("Creating sample data for development...");
        
        // Create sample orders CSV
        String ordersSample = "order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date\n" +
                            "e481f51cbdc54678b7cc49136f2d6af7,9ef432eb6251297304e76186b10a928d,delivered,2017-10-02 10:56:33,2017-10-02 11:07:15,2017-10-04 19:55:00,2017-10-10 21:25:13,2017-10-18 00:00:00\n" +
                            "53cdb2fc8bc7dce0b6741e2150273451,b0830fb4747a6c6d20dea0b8c802d7ef,shipped,2018-07-24 20:41:37,2018-07-26 03:24:27,2018-07-26 14:31:00,,2018-08-08 00:00:00\n" +
                            "47770eb9100c2d0c44946d9cf07ec65d,41ce2a54c0b03bf3443c3d931a367089,delivered,2018-08-08 08:38:49,2018-08-08 08:55:23,2018-08-08 13:50:00,2018-08-17 18:06:29,2018-08-27 00:00:00";
        
        Files.writeString(Paths.get(rawDataPath + "olist_orders_dataset.csv"), ordersSample);
        
        // Create sample customers CSV
        String customersSample = "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\n" +
                               "9ef432eb6251297304e76186b10a928d,96c5e6a3f5b5f8b7e8d7c6b5a4f3e2d1c,01311,sao paulo,SP\n" +
                               "b0830fb4747a6c6d20dea0b8c802d7ef,87b5a6c4d3e2f1a9b8c7d6e5f4a3b2c1,20031,rio de janeiro,RJ\n" +
                               "41ce2a54c0b03bf3443c3d931a367089,76a5b4c3d2e1f9a8b7c6d5e4f3a2b1c0,30130,belo horizonte,MG";
        
        Files.writeString(Paths.get(rawDataPath + "olist_customers_dataset.csv"), customersSample);
        
        // Create sample products CSV
        String productsSample = "product_id,product_category_name,product_name_lenght,product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm\n" +
                              "1e9e8ef04dbcff4541ed26657ea517e5,perfumaria,40,287,1,225,16,10,14\n" +
                              "3aa071139cb16b67ca9e5dea641aaa2f,artes,44,276,1,1000,30,18,20\n" +
                              "96bd76ec8810374ed1b65e291975717f,esporte_lazer,46,250,1,154,18,9,11";
        
        Files.writeString(Paths.get(rawDataPath + "olist_products_dataset.csv"), productsSample);
        
        // Create sample orderItems CSV
        String orderItemsSample = "order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value\n" +
                                 "e481f51cbdc54678b7cc49136f2d6af7,1,1e9e8ef04dbcff4541ed26657ea517e5,4f3b6e2d1c0a9b8c7d6e5f4a3b2c1d0e,2017-10-05 00:00:00,115.98,19.99\n" +
                                 "e481f51cbdc54678b7cc49136f2d6af7,2,3aa071139cb16b67ca9e5dea641aaa2f,5e4f3d2c1b0a987654321fedcba98765,2017-10-05 00:00:00,89.90,15.50\n" +
                                 "53cdb2fc8bc7dce0b6741e2150273451,1,96bd76ec8810374ed1b65e291975717f,6f5e4d3c2b1a09876543210fedcba987,2018-07-27 00:00:00,120.50,25.00";

        Files.writeString(Paths.get(rawDataPath + "olist_order_items_dataset.csv"), orderItemsSample);
        
     // Add to createSampleData()
        String orderPaymentsSample = "order_id,payment_sequential,payment_type,payment_installments,payment_value\n" +
                                    "e481f51cbdc54678b7cc49136f2d6af7,1,credit_card,8,115.98\n" +
                                    "53cdb2fc8bc7dce0b6741e2150273451,1,boleto,1,120.50\n" +
                                    "47770eb9100c2d0c44946d9cf07ec65d,1,credit_card,10,134.90\n" +
                                    "47770eb9100c2d0c44946d9cf07ec65d,2,voucher,1,10.00";

        Files.writeString(Paths.get(rawDataPath + "olist_order_payments_dataset.csv"), orderPaymentsSample);
        
        String orderReviewsSample = "review_id,order_id,review_score,review_comment_title,review_comment_message,review_creation_date,review_answer_timestamp\n" +
                "e4d3a8f1c2b0a9d8e7f6c5b4a3d2e1f0,e481f51cbdc54678b7cc49136f2d6af7,5,\"Great product!\",\"Fast delivery and excellent quality.\",2017-10-11 10:00:00,2017-10-12 15:30:00\n" +
                "f5e4d3c2b1a0f9e8d7c6b5a4f3e2d1c0,47770eb9100c2d0c44946d9cf07ec65d,4,\"Good\",null,2018-08-18 09:15:00,2018-08-19 11:20:00";

        Files.writeString(Paths.get(rawDataPath + "olist_order_reviews_dataset.csv"), orderReviewsSample);
        
        String sellersSample = "seller_id,seller_zip_code_prefix,seller_city,seller_state\n" +
                "4f3b6e2d1c0a9b8c7d6e5f4a3b2c1d0e,01311,sao paulo,SP\n" +
                "5e4f3d2c1b0a987654321fedcba98765,20031,rio de janeiro,RJ\n" +
                "6f5e4d3c2b1a09876543210fedcba987,30130,belo horizonte,MG";

        Files.writeString(Paths.get(rawDataPath + "olist_sellers_dataset.csv"), sellersSample);

        String translationSample = "product_category_name,product_category_name_english\n" +
        		"perfumaria,perfumery\n" +
        		"artes,art\n" +
        		"esporte_lazer,sports_leisure";

        Files.writeString(Paths.get(rawDataPath + "product_category_name_translation.csv"), translationSample);
        
     // Add to createSampleData()
        String geolocationSample = "geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,geolocation_state\n" +
                                  "01311,-23.5505,-46.6333,sao paulo,SP\n" +
                                  "20031,-22.9068,-43.1729,rio de janeiro,RJ\n" +
                                  "30130,-19.9167,-43.9345,belo horizonte,MG\n" +
                                  "79090,-20.4428,-54.6464,campo grande,MS"; // MS spans timezones

        Files.writeString(Paths.get(rawDataPath + "olist_geolocation_dataset.csv"), geolocationSample);
        
        logger.info("Created sample data files for development");
    }
    
    // Method for actual Kaggle download (when you have API credentials)
    private static void downloadFromKaggle(String rawDataPath, String zipFilePath) throws IOException {
        logger.info("Downloading from Kaggle...");
        
        // This requires Kaggle API setup
        // 1. Install kaggle: pip install kaggle
        // 2. Setup API token: ~/.kaggle/kaggle.json
        // 3. Use: kaggle datasets download -d olistbr/brazilian-ecommerce
        
        // For now, we'll skip actual download and use sample data
        throw new UnsupportedOperationException("Kaggle download requires API setup. Using sample data instead.");
    }
    
    private static void extractZip(String zipFilePath, String extractPath) throws IOException {
        logger.info("Extracting ZIP file: {}", zipFilePath);
        
        byte[] buffer = new byte[1024];
        
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFilePath))) {
            ZipEntry zipEntry = zis.getNextEntry();
            
            while (zipEntry != null) {
                Path newFile = Paths.get(extractPath, zipEntry.getName());
                
                // Create directories if needed
                if (zipEntry.isDirectory()) {
                    Files.createDirectories(newFile);
                } else {
                    // Create parent directories
                    Files.createDirectories(newFile.getParent());
                    
                    // Extract file
                    try (FileOutputStream fos = new FileOutputStream(newFile.toFile())) {
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                }
                
                zipEntry = zis.getNextEntry();
            }
        }
        
        logger.info("Extraction completed");
    }
}