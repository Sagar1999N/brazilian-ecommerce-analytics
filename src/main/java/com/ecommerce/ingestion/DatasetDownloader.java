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