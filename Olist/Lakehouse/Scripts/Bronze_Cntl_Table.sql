--schema 
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'olist')
BEGIN
    DROP SCHEMA olist;
END;

CREATE SCHEMA olist;

--bronze_cntl_table
IF OBJECT_ID('olist.bronze_cntl_table', 'U') IS NOT NULL
BEGIN
    DROP TABLE olist.bronze_cntl_table;
END;

CREATE TABLE olist.bronze_cntl_table (
  sys_name VARCHAR(25) NOT NULL,
  app_name VARCHAR(25) NOT NULL,
  inbound_bucket VARCHAR(25) NOT NULL,
  raw_file_path VARCHAR(255) NOT NULL,
  raw_file_name VARCHAR(50) NOT NULL,
  raw_file_format VARCHAR(10) NOT NULL,
  delimiter VARCHAR(5) NOT NULL,
  columns_list VARCHAR(999) NOT NULL,
  pkey VARCHAR(50) NOT NULL,
  target_bucket VARCHAR(25) NOT NULL,
  bronze_schema_path VARCHAR(255) NOT NULL,
  bronze_refschema_file VARCHAR(255) NOT NULL,  
  bronze_file_path VARCHAR(255) NOT NULL,
  bronze_file_name VARCHAR(255) NOT NULL,
  bronze_file_copy_log_path VARCHAR(255) NOT NULL,  
  source_file_delete_log_path VARCHAR(255) NOT NULL,      
  bronze_archive_table_path VARCHAR(255) NOT NULL,
  bronze_archive_table_name VARCHAR(255) NOT NULL,  
  silver_table_path VARCHAR(255) NOT NULL,   
  silver_table_name VARCHAR(255) NOT NULL, 
  load_order INT NOT NULL,
  is_enabled BIT NOT NULL,
  ing_status VARCHAR(25) NOT NULL,
  last_attempt_date DATETIME NULL, 
  insert_dt DATETIME NULL,
  inserted_by VARCHAR(25) NULL,
  update_dt DATETIME NULL,
  updated_by VARCHAR(25) NULL
);

--insert
INSERT INTO olist.bronze_cntl_table
values 
('Olist', 'Customers', 'olist-ecomm-ecdl', 'Inbound/Olist/Customers', 'olist_customers.csv', 'csv', ',', '["customer_id", "customer_email_id", "customer_state", "customer_state_code"]',
 'customer_id', 'olist-ecomm-lakehouse', 'Bronze/Schema/Customers', 'customers_refschema.csv', 'Bronze/Staging/bronze_olist_customers', 'bronze_olist_customers', 'olist-ecomm-lakehouse/Logs/Bronze/Copy', 'olist-ecomm-lakehouse/Logs/Bronze/Delete',
 'Bronze/Archive/bronze_olist_customers_archive', 'bronze_olist_customers_archive', 'Silver/Delta/silver_olist_customers', 'silver_olist_customers', 1, 1, 'Finished', NULL, CURRENT_TIMESTAMP, 'olist_dev', NULL, NULL),
('Olist', 'Orders', 'olist-ecomm-ecdl', 'Inbound/Olist/Orders', 'olist_orders.csv', 'csv', ',', 
 '["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]', 
 'order_id, customer_id', 'olist-ecomm-lakehouse', 'Bronze/Schema/Orders', 'orders_refschema.csv', 'Bronze/Staging/bronze_olist_orders', 'bronze_olist_orders', 'olist-ecomm-lakehouse/Logs/Bronze/Copy', 'olist-ecomm-lakehouse/Logs/Bronze/Delete', 
 'Bronze/Archive/bronze_olist_orders_archive', 'bronze_olist_orders_archive', 'Silver/Delta/silver_olist_orders', 'silver_olist_orders', 2, 1, 'Finished', NULL, CURRENT_TIMESTAMP, 'olist_dev', NULL, NULL), 
('Olist', 'Products', 'olist-ecomm-ecdl', 'Inbound/Olist/Products', 'olist_products.csv', 'csv', ',', '["product_id", "seller_id", "product_category_name", "product_category_name_english", "price", "freight_value", "product_name_length", "product_description_length", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]',
 'product_id, seller_id', 'olist-ecomm-lakehouse', 'Bronze/Schema/Products', 'products_refschema.csv', 'Bronze/Staging/bronze_olist_products', 'bronze_olist_products', 'olist-ecomm-lakehouse/Logs/Bronze/Copy', 'olist-ecomm-lakehouse/Logs/Bronze/Delete',
 'Bronze/Archive/bronze_olist_products_archive', 'bronze_olist_products_archive', 'Silver/Delta/silver_olist_products', 'silver_olist_products', 3, 1, 'Finished', NULL, CURRENT_TIMESTAMP, 'olist_dev', NULL, NULL),
('Olist', 'Sellers', 'olist-ecomm-ecdl', 'Inbound/Olist/Sellers', 'olist_sellers.csv', 'csv', ',', '["seller_id", "seller_state", "seller_state_code"]', 'seller_id', 
 'olist-ecomm-lakehouse', 'Bronze/Schema/Sellers', 'sellers_refschema.csv', 'Bronze/Staging/bronze_olist_sellers', 'bronze_olist_sellers', 'olist-ecomm-lakehouse/Logs/Bronze/Copy', 'olist-ecomm-lakehouse/Logs/Bronze/Delete', 
 'Bronze/Archive/bronze_olist_sellers_archive', 'bronze_olist_sellers_archive', 'Silver/Delta/silver_olist_sellers', 'silver_olist_sellers', 4, 1, 'Finished', NULL, CURRENT_TIMESTAMP, 'olist_dev', NULL, NULL),
('Olist', 'Order_Items', 'olist-ecomm-ecdl', 'Inbound/Olist/Order_Items', 'olist_order_items.csv', 'csv', ',', 
 '["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]', 'order_id, order_item_id, product_id, seller_id', 
 'olist-ecomm-lakehouse', 'Bronze/Schema/Order_Items', 'order_items_refschema.csv', 'Bronze/Staging/bronze_olist_order_items', 'bronze_olist_order_items', 'olist-ecomm-lakehouse/Logs/Bronze/Copy', 'olist-ecomm-lakehouse/Logs/Bronze/Delete', 
 'Bronze/Archive/bronze_olist_order_items_archive', 'bronze_olist_order_items_archive', 'Silver/Delta/silver_olist_order_items', 'silver_olist_order_items', 5, 1, 'Finished', NULL, CURRENT_TIMESTAMP, 'olist_dev', NULL, NULL),
('Olist', 'Order_Payments', 'olist-ecomm-ecdl', 'Inbound/Olist/Order_Payments', 'olist_order_payments.csv', 'csv', ',', 
 '["order_id", "payment_sequential", "payment_type", "payment_value"]', 'order_id, payment_sequential', 
 'olist-ecomm-lakehouse', 'Bronze/Schema/Order_Payments', 'order_payments_refschema.csv', 'Bronze/Staging/bronze_olist_order_payments', 'bronze_olist_order_payments', 'olist-ecomm-lakehouse/Logs/Bronze/Copy', 'olist-ecomm-lakehouse/Logs/Bronze/Delete', 
 'Bronze/Archive/bronze_olist_order_payments_archive', 'bronze_olist_order_payments_archive', 'Silver/Delta/silver_olist_payments', 'silver_olist_payments', 6, 1, 'Finished', NULL, CURRENT_TIMESTAMP, 'olist_dev', NULL, NULL),
('Olist', 'Order_Ratings', 'olist-ecomm-ecdl', 'Inbound/Olist/Order_Ratings', 'olist_order_ratings.csv', 'csv', ',', 
 '["rating_id", "order_id", "rating_score", "rating_survey_creation_date", "rating_survey_answer_timestamp"]', 'rating_id, order_id', 
 'olist-ecomm-lakehouse', 'Bronze/Schema/Order_Ratings', 'order_ratings_refschema.csv', 'Bronze/Staging/bronze_olist_order_ratings', 'bronze_olist_order_ratings', 'olist-ecomm-lakehouse/Logs/Bronze/Copy', 'olist-ecomm-lakehouse/Logs/Bronze/Delete', 
 'Bronze/Archive/bronze_olist_order_ratings_archive', 'bronze_olist_order_ratings_archive', 'Silver/Delta/silver_olist_ratings', 'silver_olist_ratings', 7, 1, 'Finished', NULL, CURRENT_TIMESTAMP, 'olist_dev', NULL, NULL); 
  
  
 --ingestion_start_status_update
create procedure olist.ingestion_start_status_update
as
begin 
update olist.bronze_cntl_table set ing_status = 'Yet_To_Start' where sys_name = 'Olist' and is_enabled = 1;
end;

--ingestion_finish_status_update
create procedure olist.ingestion_finish_status_update
(
@app_name VARCHAR(25),
@load_order int,
@flag bit
)
as
begin 
update olist.bronze_cntl_table set ing_status = case when @flag = 1 then 'Finished' when @flag = 0 then 'Failed' end, last_attempt_date = GETDATE() 
where sys_name = 'Olist' and app_name = @app_name and load_order = @load_order and is_enabled = 1;
end;


--bronze_ingestion_log
IF OBJECT_ID('olist.bronze_ingestion_log', 'U') IS NOT NULL
BEGIN
    DROP TABLE olist.bronze_ingestion_log;
END;

CREATE TABLE olist.bronze_ingestion_log (
  pipeline_name VARCHAR(25) NOT NULL,
  pipeline_run_id VARCHAR(25) NOT NULL,
  sys_name VARCHAR(25) NOT NULL,
  app_name VARCHAR(25) NOT NULL,
  file_name VARCHAR(50) NOT NULL,  
  task_performed VARCHAR(25) NOT NULL,
  status VARCHAR(25) NOT NULL,
  rows_read INT NULL,
  rows_written INT NULL,
  log_timestamp DATETIME NOT NULL,
  error_message varchar(1000) NULL
);


create procedure olist.bronze_ingestion_log_update
(
 @pipeline_name VARCHAR(25),
 @pipeline_run_id VARCHAR(25),
 @sys_name VARCHAR(25),
 @app_name VARCHAR(25),
 @file_name VARCHAR(50),  
 @task_performed VARCHAR(25),
 @status VARCHAR(25),
 @rows_read INT,
 @rows_written INT,
 @log_timestamp DATETIME,
 @error_message varchar(1000)
)
as
begin
	insert
	into
	olist.bronze_ingestion_log
values
(
 @pipeline_name,
 @pipeline_run_id,
 @sys_name,
 @app_name,
 @file_name,  
 @task_performed,
 @status,
 @rows_read,
 @rows_written,
 @log_timestamp,
 @error_message
)
end;


truncate table olist.bronze_cntl_table;

truncate table olist.bronze_ingestion_log;

select * from olist.bronze_ingestion_log where pipeline_run_id = '634285fb-125b-4ae9-85bd-b' order by log_timestamp desc;

SELECT ing_status, last_attempt_date, * FROM olist.bronze_cntl_table where is_enabled = 1 order by load_order;

SELECT sys_name,app_name,inbound_bucket,raw_file_path,raw_file_name,raw_file_format,delimiter,columns_list,pkey,target_bucket,
bronze_schema_path,bronze_refschema_file,bronze_file_path,bronze_file_name,bronze_file_copy_log_path,source_file_delete_log_path,
load_order,is_enabled,ing_status 
FROM olist.bronze_cntl_table where is_enabled = 1 and ing_status = 'Yet_To_Start' order by load_order;

capture_validation_success:
{"PipelineName":"S3_to_ADLSGen2_demo","PipelineRunId":"809b388b-98ee-40c1-9e21-946b97383a89","JobId":"809b388b-98ee-40c1-9e21-946b97383a89","ActivityRunId":"c2905b24-29bc-41e3-ab89-e6c36b69ed21","ExecutionStartTime":"2024-07-17T09:39:23.1388032Z","ExecutionEndTime":"2024-07-17T09:39:27.5796734Z","Status":"Succeeded","Error":null,"Output":{"structure":[{"name":"customer_id","type":"String"},{"name":"customer_email_id","type":"String"},{"name":"customer_state","type":"String"},{"name":"customer_state_code","type":"String"}],"effectiveIntegrationRuntime":"AutoResolveIntegrationRuntime (South India)","executionDuration":2,"durationInQueue":{"integrationRuntimeQueue":0},"billingReference":{"activityType":"PipelineActivity","billableDuration":[{"meterType":"AzureIR","duration":0.016666666666666666,"unit":"Hours"}]}},"ExecutionDetails":{"integrationRuntime":[{"name":"AutoResolveIntegrationRuntime","type":"Managed","location":"South India"}]},"StatusCode":200,"ExecutionStatus":"Pass","Duration":"00:00:04.4408702","RecoveryStatus":"None","ActivityType":"GetMetadata"}

capture_validation_success:
{"PipelineName":"S3_to_ADLSGen2_demo","PipelineRunId":"2a15596b-22c3-49c7-8269-0b0bde27c71e","JobId":"2a15596b-22c3-49c7-8269-0b0bde27c71e","ActivityRunId":"35d60100-2be3-46d8-ba2e-fa36e19c42a8","ExecutionStartTime":"2024-07-17T10:02:11.9151845Z","ExecutionEndTime":"2024-07-17T10:02:14.8480722Z","Status":"Failed","Error":{"errorCode":"2011","message":"The operation on file olist_customers.csv under directory olist-ecomm-ecdl/Inbound/Olist/Customers is failed due to exception. ","failureType":"UserError","target":"Get Metadata1","details":[]},"Output":{"effectiveIntegrationRuntime":"AutoResolveIntegrationRuntime (South India)","executionDuration":0,"durationInQueue":{"integrationRuntimeQueue":0},"billingReference":{"activityType":"PipelineActivity","billableDuration":[{"meterType":"AzureIR","duration":0.016666666666666666,"unit":"Hours"}]}},"ExecutionDetails":{"integrationRuntime":[{"name":"AutoResolveIntegrationRuntime","type":"Managed","location":"South India"}]},"StatusCode":400,"ExecutionStatus":"Fail","Duration":"00:00:02.9328877","RecoveryStatus":"None","ActivityType":"GetMetadata"}

capture_copy_success:
{"PipelineName":"S3_to_ADLSGen2_demo","PipelineRunId":"b1521a03-36be-495a-b70f-1ad653ee4bf1","JobId":"b1521a03-36be-495a-b70f-1ad653ee4bf1","ActivityRunId":"4131ef54-cf1b-4cec-a68b-e505a8e3bb7e","ExecutionStartTime":"2024-07-17T10:08:47.8114657Z","ExecutionEndTime":"2024-07-17T10:09:09.6349389Z","Status":"Succeeded","Error":null,"Output":{"dataRead":9892817,"dataWritten":7105874,"filesRead":1,"filesWritten":1,"sourcePeakConnections":1,"sinkPeakConnections":1,"rowsRead":99441,"rowsCopied":99441,"copyDuration":19,"throughput":1099.202,"logFilePath":"olist-ecomm-lakehouse/Logs/Bronze/Copy/copyactivity-logs/Copy data1/4131ef54-cf1b-4cec-a68b-e505a8e3bb7e/","errors":[],"effectiveIntegrationRuntime":"AutoResolveIntegrationRuntime (South India)","usedDataIntegrationUnits":4,"billingReference":{"activityType":"DataMovement","billableDuration":[{"meterType":"AzureIR","duration":0.06666666666666667,"unit":"DIUHours"}],"totalBillableDuration":[{"meterType":"AzureIR","duration":0.06666666666666667,"unit":"DIUHours"}]},"usedParallelCopies":1,"executionDetails":[{"source":{"type":"AmazonS3"},"sink":{"type":"AzureBlobFS","region":"South India"},"status":"Succeeded","start":"2024-07-17T10:08:48.930646Z","duration":19,"usedDataIntegrationUnits":4,"usedParallelCopies":1,"profile":{"queue":{"status":"Completed","duration":8},"transfer":{"status":"Completed","duration":9,"details":{"listingSource":{"type":"AmazonS3","workingDuration":1},"readingFromSource":{"type":"AmazonS3","workingDuration":0},"writingToSink":{"type":"AzureBlobFS","workingDuration":0}}}},"detailedDurations":{"queuingDuration":8,"transferDuration":9}}],"dataConsistencyVerification":{"VerificationResult":"NotVerified"},"durationInQueue":{"integrationRuntimeQueue":0}},"ExecutionDetails":{"integrationRuntime":[{"name":"AutoResolveIntegrationRuntime","type":"Managed","location":"South India","nodes":null}]},"StatusCode":200,"ExecutionStatus":"Pass","Duration":"00:00:21.8234732","RecoveryStatus":"None","ActivityType":"Copy"}
