# Wasabi S3 Bucket Deleter

A robust Python tool for completely deleting Wasabi S3 buckets and all their contents, including versioned objects, delete markers, and objects with special characters in their names.

## Features

- **Complete Bucket Deletion**: Removes all objects, versions, and delete markers
- **Special Character Handling**: Properly handles objects with unicode characters, spaces, and special symbols
- **Parallel Processing**: Uses multi-threading for fast deletion of large buckets
- **Automatic Retry Logic**: Implements exponential backoff for handling rate limits and server errors
- **Versioning Support**: Handles versioned buckets by suspending versioning and deleting all versions
- **Cleanup Operations**:
  - Removes bucket policies and lifecycle rules
  - Aborts incomplete multipart uploads
  - Verifies bucket is empty before deletion
- **Force Delete**: Supports Wasabi's force_delete parameter as a last resort
- **Progress Tracking**: Real-time progress updates and performance statistics
- **Error Recovery**: Automatically falls back to individual object deletion when batch operations fail

## Requirements

- Python 3.9+
- boto3 >= 1.26.0
- botocore >= 1.29.0
- requests >= 2.28.0

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/wasabi-bucket-cleaner.git
cd wasabi-bucket-cleaner
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Set your Wasabi credentials as environment variables:

```bash
export WASABI_ACCESS_KEY='your-access-key'
export WASABI_SECRET_KEY='your-secret-key'
```

## Usage

### Interactive Mode

Run the script without arguments for interactive mode:
```bash
python wasabi-deleter.py
```

The script will:
1. List all buckets in your Wasabi account
2. Ask for confirmation before deleting each bucket
3. Delete all objects, versions, and delete markers
4. Remove bucket configuration (policies, lifecycle rules)
5. Delete the empty bucket

### Command Line Options

```
usage: wasabi-deleter.py [-h] [--bucket BUCKET] [--yes] [--list] [--force]
                         [--workers WORKERS] [--verbose]

options:
  -h, --help            show this help message and exit
  --bucket, -b BUCKET   Specific bucket to delete
  --yes, -y             Skip confirmation prompts
  --list, -l            List buckets without deleting
  --force, -f           Use Wasabi force_delete if standard delete fails
  --workers, -w WORKERS Number of concurrent workers (default: 10)
  --verbose, -v         Enable verbose logging
```

### Examples

```bash
# Interactive mode - list and select buckets
python wasabi-deleter.py

# Delete a specific bucket
python wasabi-deleter.py --bucket my-bucket

# Delete without confirmation prompts (use with caution!)
python wasabi-deleter.py --bucket my-bucket --yes

# List all buckets without deleting
python wasabi-deleter.py --list

# Use force delete with verbose output
python wasabi-deleter.py --bucket my-bucket --force --verbose

# Increase worker count for faster deletion
python wasabi-deleter.py --bucket my-bucket --workers 20
```

## How It Works

### 1. **Multi-Region Support**
The script automatically detects the region of each bucket and uses the appropriate regional endpoint for optimal performance.

### 2. **Batch Deletion with Fallback**
- Attempts to delete objects in batches of 500 for efficiency
- If batch deletion fails (e.g., due to special characters causing MalformedXML errors), automatically falls back to individual object deletion
- Implements retry logic with exponential backoff for handling temporary failures

### 3. **Handling Versioned Buckets**
- Detects if versioning is enabled
- Suspends versioning to prevent new versions during deletion
- Deletes all object versions and delete markers
- Performs multiple passes to ensure all versions are removed

### 4. **Error Handling**
- **MalformedXML Errors**: Automatically switches to individual object deletion
- **Server Errors (500)**: Implements aggressive retry with exponential backoff
- **Rate Limiting**: Adds delays between requests to avoid overwhelming the service
- **Partial Failures**: Tracks and retries failed deletions

### 5. **Cleanup Operations**
Before deleting objects:
- Removes bucket policies that might interfere with deletion
- Deletes lifecycle rules
- Aborts incomplete multipart uploads

After deletion attempts:
- Verifies the bucket is truly empty
- Performs a final aggressive cleanup if needed
- Uses Wasabi's force_delete parameter as a last resort

## Performance Tuning

The script uses a `WasabiConfig` dataclass with the following configurable parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_workers` | 10 | Number of concurrent threads (adjustable via `--workers` flag) |
| `batch_size` | 500 | Objects per batch deletion request |
| `max_retries` | 5 | Maximum retry attempts for failed operations |
| `retry_delay` | 1.0 | Initial retry delay in seconds |
| `max_retry_delay` | 30.0 | Maximum retry delay cap |
| `connection_pool_size` | 20 | Connection pool size for HTTP connections |
| `rate_limit_delay` | 0.1 | Delay between requests to avoid rate limiting |

### Tuning Tips

- **Large buckets**: Increase `--workers` to 15-20 for faster deletion
- **Rate limiting issues**: Reduce `--workers` to 5 or less
- **Network bandwidth**: Adjust workers based on available bandwidth
- **Server errors**: The script automatically reduces workers on repeated errors

## Troubleshooting

### Common Issues

1. **"BucketNotEmpty" Error**
   - The script will automatically try force_delete
   - Check for objects with active object lock
   - Verify all delete markers are removed

2. **MalformedXML Errors**
   - Usually caused by special characters in object names
   - The script automatically handles this by switching to individual deletion

3. **Rate Limiting**
   - The script implements automatic backoff
   - Consider reducing MAX_WORKERS if you see many 503 errors

4. **Object Lock**
   - Objects with compliance mode locks cannot be deleted until the retention period expires
   - The script will warn about buckets with object lock enabled

### Debug Mode

Enable verbose/debug logging using the `--verbose` or `-v` flag:
```bash
python wasabi-deleter.py --verbose
```

This will show detailed information including:
- Object keys with special characters
- Individual deletion attempts
- Retry operations and timing

## Security Considerations

- Never commit credentials to version control
- Use IAM policies to limit access to specific buckets
- Consider using temporary credentials for enhanced security
- The script requires the following S3 permissions:
  - `s3:ListBucket`
  - `s3:ListBucketVersions`
  - `s3:DeleteObject`
  - `s3:DeleteObjectVersion`
  - `s3:DeleteBucket`
  - `s3:GetBucketLocation`
  - `s3:GetBucketVersioning`
  - `s3:PutBucketVersioning`
  - `s3:DeleteBucketPolicy`
  - `s3:DeleteBucketLifecycle`
  - `s3:AbortMultipartUpload`
  - `s3:ListMultipartUploads`

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This tool performs destructive operations that cannot be undone. Always ensure you have backups of important data before using this tool. The authors are not responsible for any data loss resulting from the use of this software.

## Acknowledgments

- Built using the excellent [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) library
- Inspired by the challenges of managing large S3-compatible storage buckets
- Special thanks to the Wasabi support team for their API documentation