from awsglue.transforms import ApplyMapping
from awsglue.dynamicframe import DynamicFrame

# Create a dynamic frame from your data source
source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="your_database",
    table_name="your_table"
)

# Define your mapping list with the new column
mapping = [
    ("old_column_name", "string", "new_column_name", "int"),  # Existing mapping
    ("", "int", "new_column_name_2", "string")  # New column mapping
    # (source_column, source_type, target_column, target_type)
]

# Apply the mapping and create a new dynamic frame
mapped_dyf = ApplyMapping.apply(
    frame=source_dyf,
    mappings=mapping,
    transformation_ctx="mapped_dyf",
    add_column="new_column_name_2:int"
)

# Convert the dynamic frame back to a DataFrame
df = mapped_dyf.toDF()

# Print the schema of the DataFrame to verify the changes
df.printSchema()
