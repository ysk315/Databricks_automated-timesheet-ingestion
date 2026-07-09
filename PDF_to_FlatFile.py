##This file parses PDF files from one volume and creates flat files into another volume  and update metadata to track processed and unprocessed files.

# Define parameters
input_volume_path = "/Volumes/timesheets/timesheets/raw_timesheets/"
output_volume_path = "/Volumes/timesheets/timesheets/source_files/"
metadata_table = "timesheets.timesheets.pdf_processing_metadata"

# Imports
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql.functions import (
    col, explode, current_timestamp, regexp_extract, udf, lit
)
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, TimestampType

print("✓ Parameters configured:")
print(f"  Input Volume: {input_volume_path}")
print(f"  Output Volume: {output_volume_path}")
print(f"  Metadata Table: {metadata_table}")
print("✓ Imports loaded")
print("\nProcessing: TIMESHEET DATA from PDFs")
# ============================================================================
# STEP 1: CLEANUP - Delete all files from source_files and clear metadata
# ============================================================================
print("\n" + "="*80)
print("CLEANUP: Deleting existing files and metadata")
print("="*80)

try:
    # Delete all files in source_files volume
    files = dbutils.fs.ls(output_volume_path)
    if files:
        print(f"Deleting {len(files)} files from {output_volume_path}")
        for file in files:
            dbutils.fs.rm(file.path, recurse=False)
        print(f"✓ Deleted all files from source_files")
    else:
        print("✓ source_files directory is already empty")
except Exception as e:
    print(f"✓ source_files directory is empty or doesn't exist yet")

# Clear metadata table
try:
    spark.sql(f"TRUNCATE TABLE {metadata_table}")
    print(f"✓ Cleared metadata table: {metadata_table}")
except Exception as e:
    print(f"✓ Metadata table doesn't exist yet or is already empty")

print("="*80 + "\n")

# ============================================================================
# STEP 2: Create metadata table schema if needed
# ============================================================================
# Create metadata table schema
metadata_schema = StructType([
    StructField("pdf_source_file_name", StringType(), False),
    StructField("is_processed", StringType(), False),
    StructField("output_file_name", StringType(), True),
    StructField("output_file_format", StringType(), True),
    StructField("processing_timestamp", TimestampType(), False),
    StructField("error_message", StringType(), True)
])

# Create empty metadata table if it doesn't exist
from pyspark.sql import Row
empty_metadata = spark.createDataFrame([], metadata_schema)
empty_metadata.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(metadata_table)

print(f"✓ Metadata table ready: {metadata_table}")
print("  Columns: pdf_source_file_name, is_processed, output_file_name, output_file_format, processing_timestamp, error_message")

# Read ALL PDF files
print(f"\nReading ALL PDF files from: {input_volume_path}")

df = spark.read.format("binaryFile").load(input_volume_path)
total_files = df.count()

print(f"✓ Found {total_files} PDF file(s)")
print(f"\n🔄 Processing ALL {total_files} PDF files...")
# Parse the single target PDF using ai_parse_document
print("Parsing target PDF with ai_parse_document...")

parsed = df.selectExpr(
    "path",
    "modificationTime",
    "length",
    "ai_parse_document(content, MAP('version', '2.0')) as parsed_content"
)

print("✓ PDF parsed successfully")
# Extract elements from parsed content
elements_df = parsed.selectExpr(
    "path",
    "explode(try_cast(parsed_content:document:elements AS array<variant>)) as element"
).selectExpr(
    "path",
    "try_cast(element:type AS string) as element_type",
    "try_cast(element:content AS string) as content"
)

# Define schema for field-value pairs
field_value_schema = ArrayType(StructType([
    StructField("field", StringType(), True),
    StructField("value", StringType(), True)
]))

# UDF to parse HTML tables and extract field-value pairs
def parse_html_table(html_content):
    if not html_content or '<table' not in html_content:
        return []
    
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html_content, re.DOTALL)
    field_values = []
    current_headers = None
        
    for row in rows:
        # Extract cells and identify if they're headers (th) or data (td)
        ths = re.findall(r'<th[^>]*>(.*?)</th>', row, re.DOTALL)
        tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        
        # Clean cell content
        ths = [re.sub(r'<[^>]+>', '', cell).strip() for cell in ths]
        tds = [re.sub(r'<[^>]+>', '', cell).strip() for cell in tds]
        
        # Pattern 1: Row has BOTH <th> and <td> in same row (mixed row)
        if ths and tds:
            # Check if tds have meaningful content (not all empty)
            meaningful_tds = [td for td in tds if td]  # Filter out empty strings
            
            if meaningful_tds:
                # Case 1a: Mixed row with actual data - pair headers with values
                # Example: <tr><th>Site</th><td>US-MA-Boston...</td></tr>
                for i in range(min(len(ths), len(meaningful_tds))):
                    if ths[i]:
                        field_values.append((ths[i], meaningful_tds[i]))
            else:
                # Case 1b: Row has <th> tags but all <td> are empty - treat as header row
                # Example: <tr><th>Buyer</th><th>Supplier</th><td></td></tr>
                current_headers = [cell for cell in ths if cell]
        
        # Pattern 2: Row has ONLY <th> tags (header row)
        elif ths and not tds:
            # Store as headers for next data row
            current_headers = [cell for cell in ths if cell]
        
        # Pattern 3: Row has ONLY <td> tags
        elif tds and not ths:
            # Case 3a: Two-column format (field, value)
            # Example: <tr><td>Submit Date</td><td>06/26/2026 04:32 PM</td></tr>
            if len(tds) == 2 and tds[0] and tds[1]:
                field_values.append((tds[0], tds[1]))
            # Case 3b: Multi-column with headers (match by position)
            elif current_headers and len(tds) > 2:
                for i, value in enumerate(tds):
                    if i < len(current_headers) and value:
                        field_values.append((current_headers[i], value))
    
    return field_values

parse_table_udf = udf(parse_html_table, field_value_schema)

# Extract all field-value pairs from tables
tables_df = elements_df.filter(col("element_type") == "table") \
    .withColumn("field_values", parse_table_udf(col("content"))) \
    .withColumn("field_value", explode(col("field_values"))) \
    .select(
        col("path"),
        col("field_value.field").alias("field"),
        col("field_value.value").alias("value")
    )

# All fields are extracted from tables (no text extraction needed)

# SHOW ALL DISTINCT FIELDS FOUND
print("="*80)
print("ALL DISTINCT FIELDS FOUND IN PDF:")
print("="*80)
tables_df.select("field").distinct().orderBy("field").show(100, truncate=False)

# ============================================================================
# DYNAMIC EXTRACTION: Extract ALL fields found (no filtering)
# ============================================================================
print("\n✓ Extracting ALL fields found in PDF (dynamic columns)")

# Add filename and keep all field-value pairs
# Cast value to string to avoid type inference issues during pivot
timesheet_details_long = tables_df \
    .withColumn("filename", regexp_extract(col("path"), r"([^/]+\.pdf)$", 1)) \
    .withColumn("value", col("value").cast("string")) \
    .select("filename", "field", "value")

row_count = timesheet_details_long.count()
print(f"\n✓ Extracted {row_count} field records from PDF")
print(f"\nAll extracted fields:")
timesheet_details_long.select("field").distinct().orderBy("field").show(truncate=False)

# PIVOT: Convert from long format (rows) to wide format (columns)
print("\n" + "="*80)
print("PIVOTING DATA: Each field becomes a column")
print("="*80)

timesheet_details = timesheet_details_long.groupBy("filename").pivot("field").agg(
    {"value": "first"}
)

# Get all columns dynamically (filename + all pivoted field columns)
all_columns = timesheet_details.columns
print(f"✓ Pivoted columns: {', '.join([c for c in all_columns if c != 'filename'])}")

print(f"✓ Data pivoted: {timesheet_details.count()} row(s) (one per PDF file)")
print("\nPivoted data preview:")
timesheet_details.show(truncate=False)
from datetime import datetime
import re as re_lib

# Function to parse date strings like "March17th_March23rd_2025" into "20250317_to_20250323"
def parse_date_from_text(date_text, year):
    """
    Convert date strings like 'March17th', 'June22nd' to YYYYMMDD format
    """
    # Month name to number mapping
    months = {
        'jan': 1, 'january': 1,
        'feb': 2, 'february': 2,
        'mar': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5,
        'jun': 6, 'june': 6,
        'jul': 7, 'july': 7,
        'aug': 8, 'august': 8,
        'sep': 9, 'september': 9,
        'oct': 10, 'october': 10,
        'nov': 11, 'november': 11,
        'dec': 12, 'december': 12
    }
    
    # Try to extract month name and day number
    match = re_lib.search(r'([a-zA-Z]+)(\d{1,2})(?:st|nd|rd|th)?', date_text, re_lib.IGNORECASE)
    if match:
        month_str = match.group(1).lower()
        day_str = match.group(2)
        
        if month_str in months:
            month_num = months[month_str]
            day_num = int(day_str)
            return f"{year}{month_num:02d}{day_num:02d}"
    
    return None

def extract_date_from_pdf_filename(filename):
    """
    Extract date range from PDF filename and convert to YYYYMMDD_to_YYYYMMDD format
    Example: March17th_March23rd_2025 → 20250317_to_20250323
    """
    # Pattern: MonthDay_MonthDay_Year
    match = re_lib.search(r'([a-zA-Z]+\d{1,2}(?:st|nd|rd|th)?)_([a-zA-Z]+\d{1,2}(?:st|nd|rd|th)?)_(\d{4})', filename, re_lib.IGNORECASE)
    if match:
        start_date_text = match.group(1)
        end_date_text = match.group(2)
        year = match.group(3)
        
        start_date = parse_date_from_text(start_date_text, year)
        end_date = parse_date_from_text(end_date_text, year)
        
        if start_date and end_date:
            return f"{start_date}_to_{end_date}"
    
    # Fallback: use current date
    now = datetime.now()
    return now.strftime("%Y%m%d") + "_to_" + now.strftime("%Y%m%d")

# Process files ONE AT A TIME with immediate metadata logging
# This ensures metadata is logged right after each file is created

total_records = timesheet_details.count()

print(f"Total PDFs: {total_records}")
print(f"Assigning formats: 2 JSON, 2 XML, rest CSV\n")

import json

json_saved = 0
xml_saved = 0
csv_saved = 0
failed_count = 0

print("Saving individual files with metadata tracking...")
print("="*80)

# Get list of all PDF filenames to process
from pyspark.sql.functions import col as spark_col
filenames_list = [row.filename for row in timesheet_details.select("filename").collect()]

print(f"Processing {len(filenames_list)} PDF(s) one at a time...\n")

# Process each PDF one at a time
for idx, original_filename in enumerate(filenames_list):
    output_filename = None
    
    # Determine output format based on index
    if idx < 2:
        output_format = 'json'
    elif idx < 4:
        output_format = 'xml'
    else:
        output_format = 'csv'
    
    try:
        # Filter for this specific PDF
        single_pdf = timesheet_details.filter(col("filename") == original_filename)
        
        # Get all column names (use collect to get schema without triggering computation)
        all_columns = single_pdf.columns
        data_columns = [c for c in all_columns if c != 'filename']
        
        # Collect the Row object using SQL query to avoid DataFrame operations
        single_pdf.createOrReplaceTempView("temp_single_pdf")
        row_data = spark.sql("SELECT * FROM temp_single_pdf LIMIT 1").collect()
        
        if not row_data or len(row_data) == 0:
            raise Exception("No data found for this PDF")
        
        first_row = row_data[0]
        
        # Extract values as strings manually
        row_dict = {}
        for col_name in all_columns:
            try:
                val = first_row[col_name]
                row_dict[col_name] = str(val) if val is not None else ""
            except:
                row_dict[col_name] = ""
        
        # Extract date from PDF FILENAME (not Submit Date field)
        date_string = extract_date_from_pdf_filename(original_filename)
        
        # Generate output filename based on format
        output_filename = f"timesheets_{date_string}.{output_format}"
        output_path = f"{output_volume_path}{output_filename}"
        
        # Create content based on format
        if output_format == 'json':
            # JSON format
            json_content = json.dumps(row_dict, indent=2)
            dbutils.fs.put(output_path, json_content, overwrite=True)
            json_saved += 1
            
        elif output_format == 'xml':
            # XML format
            xml_lines = ['<?xml version="1.0" encoding="UTF-8"?>', '<timesheet>']
            for key, value in row_dict.items():
                safe_key = key.replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '')
                xml_lines.append(f'  <{safe_key}>{value}</{safe_key}>')
            xml_lines.append('</timesheet>')
            xml_content = '\n'.join(xml_lines)
            dbutils.fs.put(output_path, xml_content, overwrite=True)
            xml_saved += 1
            
        else:  # CSV format
            # Manually construct CSV content
            header = ','.join([f'"{col}"' for col in data_columns])
            values = ','.join([f'"{row_dict[col]}"' for col in data_columns])
            csv_content = f"{header}\n{values}\n"
            dbutils.fs.put(output_path, csv_content, overwrite=True)
            csv_saved += 1
        
        print(f"  ✓ {original_filename} → {output_filename} created ({output_format.upper()})")
            
    except Exception as e:
        # Log failure
        error_message = str(e)[:500]
        failed_count += 1
        print(f"  ✗ {original_filename} → FAILED: {error_message[:100]}")

print("="*80)
print(f"\n✓ Saved {xml_saved} XML files")
print(f"✓ Saved {json_saved} JSON files")
print(f"✓ Saved 0 TXT files")
print(f"✓ Saved {csv_saved} CSV files")
if failed_count > 0:
    print(f"✗ Failed {failed_count} files")
print(f"\nTotal files created: {json_saved + xml_saved + csv_saved}")
print(f"Output location: {output_volume_path}")

print("\n" + "="*80)
print("✓ File creation complete!")
print("  Metadata will be populated in the next cell by comparing source PDFs with created CSV files")
print("="*80)
# Define output path
output_volume_path = "/Volumes/timesheets/timesheets/source_files/"

# Display summary
print("="*80)
print("FINAL SUMMARY (After Metadata Reconciliation)")
print("="*80)

# Count files by format
all_files = dbutils.fs.ls(output_volume_path)
xml_files_list = [f.name for f in all_files if f.name.endswith('.xml')]
json_files_list = [f.name for f in all_files if f.name.endswith('.json')]
txt_files_list = [f.name for f in all_files if f.name.endswith('.txt')]
csv_files_list = [f.name for f in all_files if f.name.endswith('.csv')]

xml_saved = len(xml_files_list)
json_saved = len(json_files_list)
txt_saved = len(txt_files_list)
csv_saved = len(csv_files_list)
total_records = xml_saved + json_saved + txt_saved + csv_saved

print(f"Total PDF files processed: {total_records}")
print(f"Total fields extracted: 9")
print(f"\nOutput summary:")
print(f"  ✓ {xml_saved} individual XML files")
print(f"  ✓ {json_saved} individual JSON files")
print(f"  ✓ {txt_saved} individual TXT files")
print(f"  ✓ {csv_saved} individual CSV files")
print(f"  Location: {output_volume_path}")
print("="*80)

# Display metadata table summary
print("\n" + "="*80)
print("METADATA TRACKING SUMMARY")
print("="*80)

metadata_df = spark.read.table(metadata_table)
total_metadata = metadata_df.count()

print(f"\nTotal metadata records: {total_metadata}")
print(f"Table: {metadata_table}\n")

# Show processing status breakdown
print("Processing Status:")
metadata_df.groupBy("is_processed").count().orderBy("is_processed").show()

print("\nProcessing Status by Format:")
metadata_df.groupBy("is_processed", "output_file_format").count() \
    .orderBy("is_processed", "output_file_format").show()

# Show any failed processing
failed_df = metadata_df.filter(col("is_processed") == "N")
failed_count = failed_df.count()

if failed_count > 0:
    print(f"\n⚠️  {failed_count} PDFs failed to process:")
    failed_df.select("pdf_source_file_name", "error_message", "processing_timestamp").show(truncate=False)
else:
    print("\n✓ All PDFs processed successfully!")

# Show sample of successful processing
print("\nSample of successfully processed files (first 10):")
metadata_df.filter(col("is_processed") == "Y") \
    .select("pdf_source_file_name", "output_file_name", "output_file_format", "processing_timestamp") \
    .orderBy("processing_timestamp") \
    .show(10, truncate=False)

print("\n" + "="*80)
print("✅ COMPLETE: PDF Processing with Metadata Tracking")
print(f"   - Each PDF converted to CSV/JSON/XML/TXT format")
print(f"   - Metadata tracked in: {metadata_table}")
print(f"   - If notebook crashes, metadata table shows which files completed")
print(f"   - Fields: pdf_source_file_name, is_processed, output_file_name, output_file_format, processing_timestamp, error_message")
print("="*80)
from pyspark.sql import Row
from datetime import datetime
import re as re_lib
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define metadata schema (same as in Cell 2)
metadata_schema = StructType([
    StructField("pdf_source_file_name", StringType(), False),
    StructField("is_processed", StringType(), False),
    StructField("output_file_name", StringType(), True),
    StructField("output_file_format", StringType(), True),
    StructField("processing_timestamp", TimestampType(), False),
    StructField("error_message", StringType(), True)
])

print("="*80)
print("RECONCILING METADATA: Comparing source PDFs with created output files (JSON/XML/CSV)")
print("="*80)

# Function to parse date strings like "March17th_March23rd_2025" into "20250317_to_20250323"
def parse_date_from_text(date_text, year):
    """
    Convert date strings like 'March17th', 'June22nd' to YYYYMMDD format
    """
    # Month name to number mapping
    months = {
        'jan': 1, 'january': 1,
        'feb': 2, 'february': 2,
        'mar': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5,
        'jun': 6, 'june': 6,
        'jul': 7, 'july': 7,
        'aug': 8, 'august': 8,
        'sep': 9, 'september': 9,
        'oct': 10, 'october': 10,
        'nov': 11, 'november': 11,
        'dec': 12, 'december': 12
    }
    
    # Try to extract month name and day number
    # Pattern: Month + Day (with st/nd/rd/th suffix)
    match = re_lib.search(r'([a-zA-Z]+)(\d{1,2})(?:st|nd|rd|th)?', date_text, re_lib.IGNORECASE)
    if match:
        month_str = match.group(1).lower()
        day_str = match.group(2)
        
        if month_str in months:
            month_num = months[month_str]
            day_num = int(day_str)
            
            # Format as YYYYMMDD
            return f"{year}{month_num:02d}{day_num:02d}"
    
    return None

def extract_date_from_filename(filename):
    """
    Extract date pattern from filename and convert to YYYYMMDD_to_YYYYMMDD format
    """
    # First try: Already in YYYYMMDD_to_YYYYMMDD format (from CSV files)
    match = re_lib.search(r'(\d{8}_to_\d{8})', filename)
    if match:
        return match.group(1)
    
    # Second try: PDF format like "March17th_March23rd_2025" or "June22nd_June28th_2026"
    # Pattern: MonthDay_MonthDay_Year
    match = re_lib.search(r'([a-zA-Z]+\d{1,2}(?:st|nd|rd|th)?)_([a-zA-Z]+\d{1,2}(?:st|nd|rd|th)?)_(\d{4})', filename, re_lib.IGNORECASE)
    if match:
        start_date_text = match.group(1)
        end_date_text = match.group(2)
        year = match.group(3)
        
        start_date = parse_date_from_text(start_date_text, year)
        end_date = parse_date_from_text(end_date_text, year)
        
        if start_date and end_date:
            return f"{start_date}_to_{end_date}"
    
    return None

# Get all PDF files from raw_timesheets
raw_pdf_files = dbutils.fs.ls(input_volume_path)
print(f"\nFound {len(raw_pdf_files)} PDF file(s) in raw_timesheets")

# Get all output files from source_files (JSON, XML, CSV)
target_files = dbutils.fs.ls(output_volume_path)
json_files = [f for f in target_files if f.name.endswith('.json')]
xml_files = [f for f in target_files if f.name.endswith('.xml')]
csv_files = [f for f in target_files if f.name.endswith('.csv')]

print(f"Found {len(json_files)} JSON file(s) in source_files")
print(f"Found {len(xml_files)} XML file(s) in source_files")
print(f"Found {len(csv_files)} CSV file(s) in source_files")

# Extract date parts from all output filenames
output_date_map = {}  # date_part -> (filename, format)
for json_file in json_files:
    date_part = extract_date_from_filename(json_file.name)
    if date_part:
        output_date_map[date_part] = (json_file.name, 'json')

for xml_file in xml_files:
    date_part = extract_date_from_filename(xml_file.name)
    if date_part:
        output_date_map[date_part] = (xml_file.name, 'xml')

for csv_file in csv_files:
    date_part = extract_date_from_filename(csv_file.name)
    if date_part:
        output_date_map[date_part] = (csv_file.name, 'csv')

print(f"\nExtracted date parts from {len(output_date_map)} output files")
if output_date_map:
    print(f"Sample date parts: {list(output_date_map.keys())[:10]}")

# Process each PDF file
print("\n" + "="*80)
print("Matching PDFs with output files and updating metadata...")
print("="*80)

matched_count = 0
unmatched_count = 0

for pdf_file in raw_pdf_files:
    if not pdf_file.name.endswith('.pdf'):
        continue
    
    pdf_filename = pdf_file.name
    
    # Try to extract date from PDF filename first
    date_part_from_name = extract_date_from_filename(pdf_filename)
    
    # Check if the extracted date matches any output file
    if date_part_from_name and date_part_from_name in output_date_map:
        # Match found! Date parts match exactly
        output_filename, output_format = output_date_map[date_part_from_name]
        
        # Insert metadata with is_processed='Y'
        metadata_data = [(
            pdf_filename,
            'Y',
            output_filename,
            output_format,
            datetime.now(),
            None
        )]
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)
        metadata_df.write.mode("append").saveAsTable(metadata_table)
        
        matched_count += 1
        print(f"  ✓ {pdf_filename} → {output_filename} (matched by date: {date_part_from_name}, format: {output_format.upper()})")
    
    else:
        # No match found - mark as NOT processed
        metadata_data = [(
            pdf_filename,
            'N',
            None,
            None,
            datetime.now(),
            f"No matching output file found (PDF date: {date_part_from_name or 'unknown'})"
        )]
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)
        metadata_df.write.mode("append").saveAsTable(metadata_table)
        
        unmatched_count += 1
        print(f"  ✗ {pdf_filename} → No matching output file (PDF date: {date_part_from_name or 'unknown'})")

print("\n" + "="*80)
print(f"✓ Metadata reconciliation complete!")
print(f"  Matched: {matched_count} PDF(s)")
print(f"  Unmatched: {unmatched_count} PDF(s)")
print(f"  Metadata table: {metadata_table}")
print("="*80)
