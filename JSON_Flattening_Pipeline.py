import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load student data from JSON file
with open('/content/student.json', 'r') as f:
    data = json.load(f)

# Create Spark DataFrame from loaded JSON
df = spark.createDataFrame(data)

# Display DataFrame schema and data
df.printSchema()
df.show(truncate=False)

# Flatten nested fields and explode subjects array
flat_df = df.select(
    col("student_id"),
    col("name.first").alias("first_name"),
    col("name.last").alias("last_name"),
    col("contact.email").alias("email"),
    col("contact.phone").alias("phone"),
    explode(col("subjects")).alias("subject")
).select(
    col("student_id"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("phone"),
    col("subject.name").alias("subject_name"),
    col("subject.score").alias("subject_score")
)

# Show the flattened DataFrame
flat_df.show()

# Write the flattened DataFrame to Parquet format
output_path = "/content/flattened_students_parquet"
flat_df.write.mode("overwrite").parquet(output_path)

# Register the Parquet file as an external Spark SQL table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS flattened_students
USING PARQUET
OPTIONS (
  path '{output_path}'
)
""")

# Refresh the table and display its contents
spark.sql("REFRESH TABLE flattened_students")
spark.sql("SELECT * FROM flattened_students").show()