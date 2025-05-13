# Wasabi S3 Bucket Deleter

A Python script to completely empty and delete Wasabi S3 buckets with proper handling of versioned objects and delete markers.

## Why This Tool Exists

This utility was created to address the significant challenges when trying to delete all objects from a Wasabi S3 bucket, especially for buckets with:
- Large numbers of objects
- Versioned objects
- Delete markers
- Objects across multiple regions

The Wasabi console and standard S3 tools often struggle with these scenarios, making bucket cleanup a time-consuming and error-prone process. This script offers a reliable solution that properly handles these edge cases.

## Features

- Lists all buckets in your Wasabi account
- Handles object versioning correctly (deletes all versions and delete markers)
- Works with all Wasabi regions, automatically detecting bucket region
- Interactive confirmation prompts for safety
- Efficient batch processing with support for large buckets

## Prerequisites

- Python 3.6+
- Boto3 library (`pip install boto3`)
- Wasabi account with access and secret keys

## Configuration

Edit the following variables in `wasabi_s3_configuration.py`:

```python
WASABI_ACCESS_KEY = 'your-access-key'
WASABI_SECRET_KEY = 'your-secret-key'
```

## Usage

1. Configure your Wasabi credentials in the script
2. Run the script:

```
python wasabi_s3_configuration.py
```

3. The script will display available buckets and prompt for confirmation before deleting

## How It Works

1. Lists all buckets in your Wasabi account
2. For each bucket, asks for confirmation before proceeding
3. If confirmed, deletes all object versions and delete markers in chunks of 1000
4. After emptying the bucket, prompts for confirmation before bucket deletion
5. Deletes the bucket if confirmed

## Safety Features

- Confirmation prompts at each major step
- Ability to skip individual buckets
- Proper error handling for failed operations

## License

See the [LICENSE](LICENSE) file for details.