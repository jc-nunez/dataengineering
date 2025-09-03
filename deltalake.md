# PySpark 3.5 with Delta Lake on Azure Blob Storage - Azure CLI Authentication

## 📋 Overview

This notebook demonstrates how to:
- Setup PySpark 3.5 with Delta Lake 3.3.2 on Azure Blob Storage
- Authenticate using Azure CLI with automatic SAS token generation
- Read Delta Lake tables and perform analysis
- Execute SQL queries with soft-delete record counting

**Prerequisites:**
- Azure Storage Account with ADLS Gen2 enabled
- Azure CLI installed and authenticated (`az login`)
- "Storage Blob Delegator" role on the storage account
- Java 11+ installed locally

---

## 🔧 Cell 1: Install Package Requirements and Dependencies

```python
# Install required packages
import subprocess
import sys

def install_packages():
    """
    Install all required packages for PySpark 3.5 + Delta Lake 3.3.2 + Azure integration.
    
    This function ensures all dependencies are installed with compatible versions.
    """
    packages = [
        "pyspark==3.5.3",
        "delta-spark==3.3.2", 
        "azure-storage-blob>=12.26.0",
        "azure-identity>=1.18.0",  # For Azure CLI authentication
        "pandas>=1.3.0",
        "pyarrow>=4.0.0"
    ]
    
    for package in packages:
        print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    
    print("✅ All packages installed successfully!")

# Uncomment the line below to install packages
# install_packages()

# Import required libraries
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum as spark_sum
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta

print("📦 All libraries imported successfully!")
```

---

## ⚙️ Cell 2: Azure CLI Configuration and Authentication

```python
# Azure Storage Configuration with Azure CLI Authentication
class AzureConfig:
    """
    Configuration class for Azure Blob Storage using Azure CLI authentication.
    
    Attributes:
        STORAGE_ACCOUNT (str): Azure Storage Account name
        CONTAINER_NAME (str): Default container name for Delta tables
        SAS_TOKEN (str): SAS token generated from Azure CLI credentials
        BASE_URL (str): Base ADLS Gen2 URL
        SAS_EXPIRY_HOURS (int): SAS token validity in hours
    """
    
    def __init__(self):
        # Storage Account Configuration
        self.STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "yourstorageaccount")
        self.CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "delta-tables")
        self.BASE_URL = f"abfss://{self.CONTAINER_NAME}@{self.STORAGE_ACCOUNT}.dfs.core.windows.net"
        
        # SAS Authentication Configuration
        self.SAS_TOKEN = os.getenv("AZURE_SAS_TOKEN", "")
        self.SAS_EXPIRY_HOURS = int(os.getenv("SAS_EXPIRY_HOURS", "2"))
        
        # Spark Configuration
        self.DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
        self.EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
        
    def display_config(self):
        """Display current configuration (hiding sensitive information)."""
        print("🔧 Configuration:")
        print(f"  Storage Account: {self.STORAGE_ACCOUNT}")
        print(f"  Container: {self.CONTAINER_NAME}")
        print(f"  Base URL: {self.BASE_URL}")
        print(f"  SAS Token: {'***' + self.SAS_TOKEN[-10:] if len(self.SAS_TOKEN) > 20 else 'NOT SET'}")
        print(f"  SAS Expiry: {self.SAS_EXPIRY_HOURS} hours")


def check_azure_cli_auth(storage_account: str) -> bool:
    """
    Check if Azure CLI is authenticated and has required permissions.
    
    Args:
        storage_account (str): Name of the storage account to test
        
    Returns:
        bool: True if authenticated and has permissions, False otherwise
    """
    try:
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient
        
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account}.blob.core.windows.net",
            credential=credential
        )
        
        # Try to get account info (this will fail if no permissions)
        account_info = blob_service_client.get_account_information()
        
        print("✅ Azure CLI authentication verified")
        print(f"   Account Kind: {account_info['account_kind']}")
        return True
        
    except Exception as e:
        print(f"❌ Azure CLI authentication failed: {str(e)}")
        print("💡 Please run 'az login' and ensure you have 'Storage Blob Delegator' role")
        return False


def generate_sas_token(config: AzureConfig) -> str:
    """
    Generate SAS token using Azure CLI authentication.
    
    Args:
        config (AzureConfig): Configuration object with storage account details
        
    Returns:
        str: Generated SAS token
        
    Raises:
        Exception: If SAS token generation fails
    """
    try:
        from azure.storage.blob import BlobServiceClient, generate_container_sas, ContainerSasPermissions
        from azure.identity import DefaultAzureCredential
        
        print(f"🔑 Generating SAS token using Azure CLI credentials...")
        
        # Calculate expiry time
        start_time = datetime.utcnow()
        expiry_time = start_time + timedelta(hours=config.SAS_EXPIRY_HOURS)
        
        # Use Azure CLI credentials
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=f"https://{config.STORAGE_ACCOUNT}.blob.core.windows.net",
            credential=credential
        )
        
        # Generate user delegation key
        user_delegation_key = blob_service_client.get_user_delegation_key(
            key_start_time=start_time,
            key_expiry_time=expiry_time
        )
        
        # Generate SAS token
        sas_token = generate_container_sas(
            account_name=config.STORAGE_ACCOUNT,
            container_name=config.CONTAINER_NAME,
            user_delegation_key=user_delegation_key,
            permission=ContainerSasPermissions(read=True, list=True, write=True),
            expiry=expiry_time
        )
        
        print(f"✅ SAS token generated successfully!")
        print(f"   Expires: {expiry_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        return sas_token
                
    except Exception as e:
        print(f"❌ Failed to generate SAS token: {str(e)}")
        print(f"💡 Solutions:")
        print(f"   1. Run 'az login' to authenticate")
        print(f"   2. Ensure you have 'Storage Blob Delegator' role")
        print(f"   3. Verify storage account name: {config.STORAGE_ACCOUNT}")
        raise


# Initialize configuration
config = AzureConfig()
config.display_config()

print(f"\n🔐 Azure CLI Setup Required:")
print(f"   1. Run: az login")
print(f"   2. Ensure you have 'Storage Blob Delegator' role")
print(f"   3. Set: export AZURE_STORAGE_ACCOUNT='{config.STORAGE_ACCOUNT}'")

# Check CLI authentication status
print(f"\n🔍 Checking Azure CLI authentication...")
cli_authenticated = check_azure_cli_auth(config.STORAGE_ACCOUNT)

# Auto-generate SAS token if CLI is authenticated and no token exists
if cli_authenticated and not config.SAS_TOKEN:
    print(f"\n🔄 Auto-generating SAS token...")
    try:
        generated_token = generate_sas_token(config)
        config.SAS_TOKEN = generated_token
        os.environ["AZURE_SAS_TOKEN"] = generated_token
        print(f"✅ SAS token auto-generated and configured!")
    except Exception as e:
        print(f"⚠️  SAS auto-generation failed: {str(e)}")
elif not cli_authenticated:
    print(f"⚠️  Please run 'az login' first to authenticate")
else:
    print(f"ℹ️  Using existing SAS token from environment")
```

---

## 🔐 Cell 3: SAS Token Management Functions

```python
def refresh_sas_token():
    """
    Generate a new SAS token using Azure CLI credentials.
    
    This function is useful when:
    - Current SAS token has expired
    - You need a new token with different expiry time
    - Starting a new development session
    
    Returns:
        str: New SAS token or None if generation failed
    """
    global config
    
    try:
        print("🔄 Generating new SAS token using Azure CLI...")
        new_token = generate_sas_token(config)
        config.SAS_TOKEN = new_token
        os.environ["AZURE_SAS_TOKEN"] = new_token
        print("✅ SAS token refreshed successfully!")
        print(f"   New token expires in {config.SAS_EXPIRY_HOURS} hours")
        return new_token
        
    except Exception as e:
        print(f"❌ Failed to refresh SAS token: {str(e)}")
        print(f"💡 Make sure 'az login' is completed and you have the right permissions")
        return None


def validate_sas_token(token: str) -> bool:
    """
    Basic validation of SAS token format.
    
    Args:
        token (str): SAS token to validate
        
    Returns:
        bool: True if token appears valid, False otherwise
    """
    if not token:
        return False
    
    required_params = ['sv', 'se', 'sr', 'sp', 'sig']
    return all(param in token for param in required_params)


def get_token_expiry(token: str) -> str:
    """
    Extract expiry time from SAS token.
    
    Args:
        token (str): SAS token
        
    Returns:
        str: Expiry time or 'Unknown' if not found
    """
    try:
        from urllib.parse import parse_qs
        params = parse_qs(token)
        if 'se' in params:
            return params['se'][0]
        return 'Unknown'
    except:
        return 'Unknown'


# Check current SAS token status
if config.SAS_TOKEN:
    print("🔍 Current SAS Token Status:")
    print(f"  Valid format: {validate_sas_token(config.SAS_TOKEN)}")
    print(f"  Expires: {get_token_expiry(config.SAS_TOKEN)}")
else:
    print("⚠️  No SAS token currently configured")

print("\n🔧 SAS token management functions ready!")
print("💡 Use refresh_sas_token() to generate a new token anytime")
```

---

## 🛠️ Cell 4: Spark Session and Common Functions

```python
def create_spark_session_with_delta(config: AzureConfig) -> SparkSession:
    """
    Create optimized Spark session with Delta Lake and SAS token authentication.
    
    Args:
        config (AzureConfig): Configuration object containing Azure settings
        
    Returns:
        SparkSession: Configured Spark session with Delta Lake support
        
    Raises:
        Exception: If Spark session creation fails or SAS token is invalid
    """
    try:
        # Generate SAS token if not available
        if not config.SAS_TOKEN:
            print("🔄 No SAS token found, generating from Azure CLI...")
            try:
                config.SAS_TOKEN = generate_sas_token(config)
                os.environ["AZURE_SAS_TOKEN"] = config.SAS_TOKEN
            except Exception:
                raise ValueError("SAS token generation failed. "
                               "Please run 'az login' and ensure you have Storage Blob Delegator role.")
        
        # Validate SAS token format
        if not validate_sas_token(config.SAS_TOKEN):
            print("⚠️  SAS token format appears invalid, but proceeding...")
        
        # Build Spark configuration
        conf = pyspark.SparkConf() \
            .setAppName("DeltaLakeAzureAnalysis") \
            .setMaster("local[*]") \
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .set("spark.driver.memory", config.DRIVER_MEMORY) \
            .set("spark.executor.memory", config.EXECUTOR_MEMORY) \
            .set("spark.sql.adaptive.enabled", "true") \
            .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .set("spark.databricks.delta.optimizeWrite.enabled", "true") \
            .set("spark.databricks.delta.autoCompact.enabled", "true") \
            .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        
        # SAS Token Authentication Configuration
        storage_account = config.STORAGE_ACCOUNT
        container_name = config.CONTAINER_NAME
        
        # Configure SAS authentication for blob and dfs endpoints
        conf.set(f"fs.azure.sas.{container_name}.{storage_account}.blob.core.windows.net", 
                 config.SAS_TOKEN)
        conf.set(f"fs.azure.sas.{container_name}.{storage_account}.dfs.core.windows.net", 
                 config.SAS_TOKEN)
        
        # Retry and timeout configurations
        conf.set("spark.hadoop.fs.azure.io.retry.max.retries", "5")
        conf.set("spark.hadoop.fs.azure.io.retry.min.backoff.interval", "3000")
        conf.set("spark.hadoop.fs.azure.io.retry.max.backoff.interval", "90000")
        
        # Build session with Delta support and Azure dependencies
        builder = SparkSession.builder.config(conf=conf)
        extra_packages = [
            "org.apache.hadoop:hadoop-azure:3.3.4",
            "org.apache.hadoop:hadoop-azure-datalake:3.3.4"
        ]
        
        spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
        
        print("✅ Spark session created with Azure CLI SAS authentication")
        print(f"   PySpark: {spark.version} | Delta Lake: enabled")
        print(f"   SAS Token: {'***' + config.SAS_TOKEN[-10:] if len(config.SAS_TOKEN) > 10 else 'INVALID'}")
        print(f"   Token expires: {get_token_expiry(config.SAS_TOKEN)}")
        
        return spark
        
    except Exception as e:
        print(f"❌ Failed to create Spark session: {str(e)}")
        raise


def test_azure_connectivity(spark: SparkSession, config: AzureConfig) -> bool:
    """
    Test connectivity to Azure Blob Storage using SAS token authentication.
    
    Args:
        spark (SparkSession): Active Spark session
        config (AzureConfig): Azure configuration with SAS token
        
    Returns:
        bool: True if connectivity test passes, False otherwise
    """
    try:
        test_path = f"{config.BASE_URL}/test-connection"
        
        # Try to write a test Delta table
        df_test = spark.range(1).toDF("test_id")
        df_test.write.format("delta").mode("overwrite").save(f"{test_path}/temp")
        
        # Try to read it back
        df_read = spark.read.format("delta").load(f"{test_path}/temp")
        count = df_read.count()
        
        print(f"✅ Azure connectivity test passed! (Read {count} record)")
        return True
        
    except Exception as e:
        print(f"❌ Azure connectivity test failed: {str(e)}")
        print(f"   Check if SAS token is valid and has read/write permissions")
        return False


def get_dataset_record_counts(spark: SparkSession, dataset_paths: Dict[str, str]) -> pd.DataFrame:
    """
    Get record counts for all datasets, grouped by _Deleted status.
    
    Args:
        spark (SparkSession): Active Spark session
        dataset_paths (Dict[str, str]): Dictionary of dataset names and their paths
        
    Returns:
        pd.DataFrame: Summary of record counts by dataset and deletion status
    """
    results = []
    
    for dataset_name, path in dataset_paths.items():
        try:
            print(f"📊 Processing {dataset_name}...")
            
            # Read Delta table
            df = spark.read.format("delta").load(path)
            
            # Check if _Deleted column exists
            if "_Deleted" not in df.columns:
                print(f"⚠️  Warning: _Deleted column not found in {dataset_name}")
                total_count = df.count()
                results.append({
                    "Dataset": dataset_name,
                    "Deleted_Status": "No _Deleted Column", 
                    "Record_Count": total_count,
                    "Percentage": 100.0
                })
            else:
                # Count records by _Deleted status
                count_df = df.groupBy("_Deleted") \
                    .agg(count("*").alias("count")) \
                    .collect()
                
                total_records = sum([row["count"] for row in count_df])
                
                for row in count_df:
                    deleted_status = "Active" if not row["_Deleted"] else "Deleted"
                    record_count = row["count"]
                    percentage = (record_count / total_records) * 100 if total_records > 0 else 0
                    
                    results.append({
                        "Dataset": dataset_name,
                        "Deleted_Status": deleted_status,
                        "Record_Count": record_count,
                        "Percentage": round(percentage, 2)
                    })
                        
        except Exception as e:
            print(f"❌ Error processing {dataset_name}: {str(e)}")
            results.append({
                "Dataset": dataset_name,
                "Deleted_Status": "Error",
                "Record_Count": 0,
                "Percentage": 0.0
            })
    
    return pd.DataFrame(results)


def create_temp_views(spark: SparkSession, datasets: Dict[str, str]) -> List[str]:
    """
    Create temporary views for all datasets.
    
    Args:
        spark (SparkSession): Active Spark session  
        datasets (Dict[str, str]): Dictionary of dataset names and paths
        
    Returns:
        List[str]: List of created view names
    """
    created_views = []
    
    for dataset_name, path in datasets.items():
        try:
            view_name = f"vw_{dataset_name.lower().replace('-', '_').replace(' ', '_')}"
            
            df = spark.read.format("delta").load(path)
            df.createOrReplaceTempView(view_name)
            
            print(f"✅ Created view: {view_name}")
            created_views.append(view_name)
            
        except Exception as e:
            print(f"❌ Failed to create view for {dataset_name}: {str(e)}")
    
    return created_views


print("🔧 Common functions defined successfully!")
print("🔑 Azure CLI SAS Token Generation Available:")
print("  • generate_sas_token(config) - Generate SAS tokens via Azure CLI")
print("  • refresh_sas_token() - Refresh current session token")  
print("  • check_azure_cli_auth() - Verify CLI authentication")
```

---

## 🚀 Cell 5: Initialize Spark Session

```python
# Create Spark session with Azure CLI SAS token authentication
spark = create_spark_session_with_delta(config)

# Test Azure connectivity
connectivity_ok = test_azure_connectivity(spark, config)

if not connectivity_ok:
    print("⚠️  Please check your SAS token and storage account settings!")
    print("   Try running refresh_sas_token() to generate a new token")
    print("   Ensure you have proper permissions on the storage account")
else:
    print("🎉 Azure CLI SAS authentication successful! Ready to process Delta tables!")
```

---

## 📁 Cell 6: Setup Datasets

```python
# Define your datasets here - Update these paths according to your Delta tables
DATASETS = {
    "customers": f"{config.BASE_URL}/tables/customers",
    "orders": f"{config.BASE_URL}/tables/orders", 
    "products": f"{config.BASE_URL}/tables/products",
    "inventory": f"{config.BASE_URL}/tables/inventory",
    "transactions": f"{config.BASE_URL}/tables/transactions"
}

print("📁 Dataset Configuration:")
print("=" * 60)
for name, path in DATASETS.items():
    print(f"  {name.ljust(15)}: {path}")

print(f"\n📊 Total datasets configured: {len(DATASETS)}")
print(f"💡 Update the DATASETS dictionary above with your actual Delta table paths!")
print(f"📍 Base URL: {config.BASE_URL}")
```

---

## 🔍 Cell 7: Create Views and Temp Views

```python
# Create temporary views for all datasets
print("🔍 Creating temporary views for datasets...")
print("=" * 50)

created_views = create_temp_views(spark, DATASETS)

if created_views:
    print(f"\n✅ Successfully created {len(created_views)} views:")
    for view in created_views:
        print(f"  • {view}")
    
    # Show available tables/views
    print(f"\n📋 Available tables and views in Spark catalog:")
    spark.sql("SHOW TABLES").show(truncate=False)
else:
    print("❌ No views were created. Please check your dataset paths and connectivity.")
    print("💡 Verify that your Delta tables exist at the configured paths")
```

---

## 📊 Cell 8: Execute SQL Queries for Record Counts

```python
# Get record counts grouped by _Deleted status
print("📊 Analyzing record counts by deletion status...")
print("=" * 60)

# Generate comprehensive record count analysis
results_df = get_dataset_record_counts(spark, DATASETS)

# Display results as a nice table
if not results_df.empty:
    print("\n📈 Record Count Analysis Results:")
    print("=" * 60)
    print(results_df.to_string(index=False))
    
    # Summary statistics
    print(f"\n📋 Summary:")
    total_records = results_df['Record_Count'].sum()
    datasets_processed = results_df['Dataset'].nunique()
    
    print(f"  • Total datasets processed: {datasets_processed}")
    print(f"  • Total records analyzed: {total_records:,}")
    
    # Group by deletion status for summary
    status_summary = results_df.groupby('Deleted_Status')['Record_Count'].sum().reset_index()
    print(f"\n🔍 Overall Status Summary:")
    for _, row in status_summary.iterrows():
        percentage = (row['Record_Count'] / total_records) * 100 if total_records > 0 else 0
        print(f"  • {row['Deleted_Status']}: {row['Record_Count']:,} records ({percentage:.1f}%)")

else:
    print("❌ No data found. Please verify your dataset paths and connectivity.")

# Execute custom SQL queries on the views
print("\n🔎 Sample SQL Queries on Created Views:")
print("=" * 50)

for view in created_views[:3]:  # Show examples for first 3 views
    try:
        print(f"\n📋 Sample query for {view}:")
        query = f"""
        SELECT 
            CASE WHEN _Deleted THEN 'Deleted' ELSE 'Active' END as Status,
            COUNT(*) as Record_Count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as Percentage
        FROM {view}
        GROUP BY _Deleted
        ORDER BY _Deleted
        """
        
        result = spark.sql(query)
        result.show()
        
    except Exception as e:
        print(f"⚠️  Could not execute query for {view}: {str(e)}")
        # Try without _Deleted column
        try:
            basic_query = f"SELECT COUNT(*) as Total_Records FROM {view}"
            basic_result = spark.sql(basic_query)
            print(f"   Basic count result:")
            basic_result.show()
        except:
            print(f"   Could not execute any query on {view}")
```

---

## 📈 Cell 9: Advanced Analysis and Cleanup

```python
# Advanced analysis queries
print("🔬 Advanced Analysis Examples:")
print("=" * 40)

# Example 1: Cross-dataset analysis (if multiple views exist)
if len(created_views) >= 2:
    try:
        cross_analysis_query = f"""
        SELECT 
            '{created_views[0]}' as Dataset_1,
            COUNT(*) as Count_1,
            '{created_views[1]}' as Dataset_2,
            (SELECT COUNT(*) FROM {created_views[1]}) as Count_2
        FROM {created_views[0]}
        """
        
        print(f"📊 Cross-dataset comparison:")
        spark.sql(cross_analysis_query).show()
        
    except Exception as e:
        print(f"⚠️  Cross-analysis failed: {str(e)}")

# Example 2: Delta table history (if tables exist)
print(f"\n📚 Delta Table History Examples:")
for dataset_name, path in list(DATASETS.items())[:2]:  # Show for first 2 datasets
    try:
        delta_table = DeltaTable.forPath(spark, path)
        print(f"\n🕐 History for {dataset_name}:")
        delta_table.history(5).select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
        
    except Exception as e:
        print(f"⚠️  Could not retrieve history for {dataset_name}: {str(e)}")

# Session summary and cleanup
print(f"\n🧹 Session Summary:")
print("=" * 30)
print(f"  • Spark Session ID: {spark.sparkContext.applicationId}")
print(f"  • Total Views Created: {len(created_views)}")
print(f"  • Datasets Configured: {len(DATASETS)}")
print(f"  • Authentication: Azure CLI + SAS Token ({config.SAS_EXPIRY_HOURS}h expiry)")
print(f"  • Token Status: {'Valid' if validate_sas_token(config.SAS_TOKEN) else 'Invalid'}")

print(f"\n💡 Useful commands:")
print(f"  • Stop Spark session: spark.stop()")
print(f"  • Refresh SAS token: refresh_sas_token()")
print(f"  • Check CLI auth: check_azure_cli_auth(config.STORAGE_ACCOUNT)")
```

---

## 🔚 Cell 10: Optional - Stop Spark Session

```python
# Uncomment to stop the Spark session
# spark.stop()
# print("🛑 Spark session stopped successfully!")
```

---

## 📋 Usage Instructions

### 🔐 **Azure CLI Authentication Setup:**

**Step 1: Authenticate with Azure CLI**
```bash
az login
```

**Step 2: Verify you have the required role**
```bash
# Check your role assignments on the storage account
az role assignment list --assignee $(az account show --query user.name -o tsv) \
  --scope /subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.Storage/storageAccounts/{storage-account}

# If you don't have "Storage Blob Delegator" role, request it from your admin
```

**Step 3: Set environment variables**
```bash
export AZURE_STORAGE_ACCOUNT='mystorageaccount'
export AZURE_CONTAINER_NAME='delta-tables'  # Optional, defaults to 'delta-tables'
export SAS_EXPIRY_HOURS='2'  # Optional, defaults to 2 hours
```

### 🚀 **Execution Steps:**
1. **Run `az login`**: Authenticate with Azure CLI
2. **Verify Permissions**: Ensure you have "Storage Blob Delegator" role
3. **Set Storage Account**: Export `AZURE_STORAGE_ACCOUNT` environment variable
4. **Update Dataset Paths**: Modify the `DATASETS` dictionary in Cell 6
5. **Run All Cells**: Execute sequentially - SAS tokens auto-generate as needed
6. **Refresh When Expired**: Use `refresh_sas_token()` when tokens expire

### 🔧 **Configuration Parameters:**
- `AZURE_STORAGE_ACCOUNT`: Your Azure Storage account name (required)
- `AZURE_CONTAINER_NAME`: Container for Delta tables (default: delta-tables)
- `SAS_EXPIRY_HOURS`: Token validity period (default: 2 hours)

### 💡 **Key Benefits of Azure CLI Approach:**
- ✅ **No secrets to manage** - Uses your Azure identity
- ✅ **Automatic token generation** - Generates SAS tokens as needed
- ✅ **Secure by default** - Leverages Azure RBAC permissions
- ✅ **Easy refresh** - Tokens regenerate automatically or via `refresh_sas_token()`

### 🛠️ **Troubleshooting:**
- **"Authentication failed"**: Run `az login` again
- **"Permission denied"**: Request "Storage Blob Delegator" role from your admin
- **"Storage account not found"**: Verify `AZURE_STORAGE_ACCOUNT` is correct
- **Token expired**: Run `refresh_sas_token()` to generate a new one

**Key Features:**
- ✅ PySpark 3.5.3 with Delta Lake 3.3.2
- 🔐 **Azure CLI authentication** (secure, no secrets)
- 🔄 **Automatic SAS token generation** and refresh
- 📊 Automated soft-delete record analysis with percentages
- 🔍 Temporary view creation for SQL queries
- 📈 Delta table operations and history analysis
- 🛠️ Comprehensive error handling and connectivity testing

---