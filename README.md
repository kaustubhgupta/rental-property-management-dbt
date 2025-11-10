## rental-property-management-dbt

### Lineage
<img width="1919" height="663" alt="image" src="https://github.com/user-attachments/assets/6bfbfda9-23c4-46ea-a4c2-4cf5dc10c67b" />

### Toolkit
- Transformation tool: dbt
- Datawarhouse: Snowflake

### Project Directory

`models/staging/aws_s3`

- All sources are defined in this folder. For this step, I am assusming that the files are uploaded in a GCS bucket and sourced to Snowflake in `AWS_S3_FILES` schema.
- Materialization defined as views, all basic level changes are done: Renaming, type casting, null handling

`models/intermediate`

- `/host`: Intermediate table will maintain the host details
- `/listings`: Listings related intermediate tables and views will be maintained

`models/mart`

The final mart table where everything joins. This table is created at listing-calendar day grain. It allows to perform period over period analysis across revenue and occupancy. All relevant intermediate tables are joined here and made as much denormalized it can go

`macros`

- `generate_schema_name`: Macro to override the default generate schema name macro. In this custom version, schema is selected based on configuration provided in `dbt_project.yml`. If schema provided in `dbt_project.yml`, then that schema is picked else it falls back to `profiles.yml` file defined schema
- `generate_targetted_array_element_distinct_values`: Macro to generate logic for getting distinct values based on array type value filers. Useful when dealing with amenities or host verification methods related queries.

`snapshots`

Amenities changelog has a consistent change timestamp and therfore has be moulded into SCD2 type implementation using timestamp strategy.To ingest this data properly, I used one adhoc script to fetch the first time changes from source file. Then snapshot was created for first time. After that I provided the full source so that old changesets are expired and new entires are created

`analysis`

- `/business_queries`: All questions queries are present here with qs and steps explantion
- `/adhoc_scripts`: Some adhoc scripts while building the pipeline