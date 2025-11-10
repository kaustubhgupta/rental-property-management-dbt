# 🏠 Rental Property Management — dbt Project

## 🧬 Lineage
<img width="1919" height="663" alt="image" src="https://github.com/user-attachments/assets/6bfbfda9-23c4-46ea-a4c2-4cf5dc10c67b" />

---

## 🧰 Toolkit
- **Transformation Tool:** dbt  
- **Data Warehouse:** Snowflake  

---

## 📂 Project Structure

### `models/staging/aws_s3`
- Contains all **source definitions**.  
- Assumes files are uploaded to an **AWS bucket** and loaded into Snowflake under the `AWS_S3_FILES` schema.  
- Models are **materialized as views**.  
- Includes **basic transformations**, such as:
  - Column renaming  
  - Type casting  
  - Null handling  

---

### `models/intermediate`
Contains intermediate transformation layers:

- **`/host`** – Stores standardized and cleaned **host-related details**.  
- **`/listings`** – Contains **listing-level transformations**

---

### `models/mart`
The **final analytical layer**, built at a **listing-calendar day grain**.

- Joins all relevant intermediate models to create a **fully denormalized table**.  
- Enables **period-over-period analysis** for key metrics such as **revenue** and **occupancy**.  

---

## ⚙️ Macros

### `generate_schema_name`
- Custom macro overriding dbt’s default schema generation behavior.  
- Selects the schema based on configuration provided in `dbt_project.yml`.  
- If no schema is specified, falls back to the default from `profiles.yml`.

### `generate_targeted_array_element_distinct_values`
- Utility macro to extract **distinct values** from **array-type fields** (e.g., `amenities`, `host_verifications`).  
- Useful for queries requiring value-based filtering on nested attributes.

---

## 🕓 Snapshots

Implements **Slowly Changing Dimension Type 2 (SCD2)** for the **amenities changelog**.

- The changelog includes a consistent change timestamp, making it ideal for timestamp-based snapshotting.  
- The initial snapshot was created using an **ad-hoc script** that captured first-time changes from the source file.  
- Subsequent runs use the full source feed to:
  - Expire outdated records  
  - Insert new change sets  

---

## 📊 Analysis

### `/business_queries`
- Contains SQL scripts addressing analytical questions with detailed step-by-step explanations.

### `/adhoc_scripts`
- Temporary or helper scripts used during pipeline development and debugging.

---

## 👥 Business Query Results

All business query results are stored as CSV under the business_queries_results folder

---

## ❄️ Snowflake Preview
<img width="450" height="328" alt="image" src="https://github.com/user-attachments/assets/6f0777a8-5130-40d1-9398-cc856271efff" />
<img width="420" height="690" alt="image" src="https://github.com/user-attachments/assets/4bf6bb7a-defd-4663-bfd8-cc54d365b677" />
<img width="403" height="535" alt="image" src="https://github.com/user-attachments/assets/57f3a714-29a0-40bb-a5eb-c746cda88ec8" />
