import asyncio
import os
import json
import shutil
import time
import cv2
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

# Constants
CACHE_DIR = 'data'
DRIVE_SIZE_CACHE_PATH = os.path.join(CACHE_DIR, 'drive_size.json')
CONTENT_STORAGE_PATH = os.path.join(CACHE_DIR, 'content_storage.json')
RCLONE_CONFIG_DIR = os.path.join(CACHE_DIR, 'rclone')

# Map of drives to their config files
DRIVE_CONFIG_MAP = {
    "shantosh": {"config": "shantosh.conf", "drive_name": "shantosh"}
    
}

# List of drive names
DRIVES = list(DRIVE_CONFIG_MAP.keys())

GB_BYTES = 1024 * 1024 * 1024
MAX_DRIVE_SIZE_GB = 95.99
FILE_CLEANUP_AGE_HOURS = 24
DOWNLOAD_BASE_DIR = "downloads"

# Create download dir if not exists
if not os.path.exists(DOWNLOAD_BASE_DIR):
    os.makedirs(DOWNLOAD_BASE_DIR)

async def run_subprocess(cmd):
    """Run a subprocess and return stdout, stderr, and return code."""
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        return stdout.decode() if stdout else "", stderr.decode() if stderr else "", process.returncode
    except Exception as e:
        logger.error(f"Error running subprocess {cmd[0]}: {e}")
        return "", str(e), 1

def bytes_to_gb(bytes_value):
    """Convert bytes to gigabytes."""
    return float(bytes_value) / GB_BYTES

async def get_thumbnail(identifier, file_path, download_dir):
    """Get thumbnail from content storage or generate from video using OpenCV."""
    thumb_path = os.path.join(download_dir, "thumb.jpg")
    
    # Try getting from content storage first
    try:
        with open(CONTENT_STORAGE_PATH, 'r') as f:
            content_data = json.load(f)
            if thumb_url := content_data[identifier].get('thumbnail'):
                curl_cmd = ['curl', '-s', '-L', '--max-time', '10', '-o', thumb_path, thumb_url]
                _, stderr, returncode = await run_subprocess(curl_cmd)
                
                if returncode == 0 and os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
                    # Re-encode the downloaded thumbnail using OpenCV to ensure compatibility
                    try:
                        img = cv2.imread(thumb_path)
                        if img is not None:
                            cv2.imwrite(thumb_path, img)
                            return thumb_path
                        else:
                            logger.error(f"Downloaded thumbnail could not be read by OpenCV for {identifier}")
                    except Exception as e:
                        logger.error(f"Error re-encoding thumbnail for {identifier}: {e}")
                else:
                    logger.error(f"Curl download failed for {identifier}: {stderr}")
    except Exception as e:
        logger.error(f"Could not get thumbnail from content storage for {identifier}, falling back to OpenCV: {e}")

    # Generate thumbnail from middle of video using OpenCV as fallback
    try:
        video = cv2.VideoCapture(file_path)
        total_frames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
        middle_frame = total_frames // 2
        video.set(cv2.CAP_PROP_POS_FRAMES, middle_frame)
        success, frame = video.read()
        video.release()

        if success:
            cv2.imwrite(thumb_path, frame)
            return thumb_path
    except Exception as e:
        logger.error(f"Error generating thumbnail: {e}")

    return None

async def read_drive_size_cache():
    """Read the drive size cache from the JSON file."""
    try:
        if os.path.exists(DRIVE_SIZE_CACHE_PATH):
            with open(DRIVE_SIZE_CACHE_PATH, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Error reading drive size cache: {e}")
        return {}

async def write_drive_size_cache(cache_data):
    """Write the drive size cache to the JSON file."""
    try:
        os.makedirs(os.path.dirname(DRIVE_SIZE_CACHE_PATH), exist_ok=True)
        with open(DRIVE_SIZE_CACHE_PATH, 'w') as f:
            json.dump(cache_data, f, indent=4)
    except Exception as e:
        logger.error(f"Error writing drive size cache: {e}")

async def update_drive_size_cache(drive, used_space_gb=None, file_size_gb=None):
    """Update the drive size cache with the latest information."""
    cache = await read_drive_size_cache()
    timestamp = datetime.now(timezone.utc).isoformat()
    
    if drive not in cache:
        cache[drive] = {'used_space_gb': 0, 'last_updated': timestamp}
    
    if used_space_gb is not None:
        cache[drive]['used_space_gb'] = used_space_gb
    elif file_size_gb is not None:
        cache[drive]['used_space_gb'] += file_size_gb
    
    cache[drive]['last_updated'] = timestamp
    await write_drive_size_cache(cache)
    return cache

async def get_drive_size(drive):
    """Get the current size information for a drive using rclone."""
    size_cmd = ['rclone', 'size', f'{drive}:', '--json']
    stdout, _, returncode = await run_subprocess(size_cmd)
    
    if returncode == 0:
        size_info = json.loads(stdout)
        return bytes_to_gb(size_info.get('bytes', 0))
    
    return None

async def get_available_drive(file_size_mb):
    """Get the first drive that has enough space for the file."""
    file_size_gb = file_size_mb / 1024
    cache = await read_drive_size_cache()
    
    # First check cache for available drives
    for drive in DRIVES:
        used_space_gb = cache.get(drive, {}).get('used_space_gb', 0)
        if (used_space_gb + file_size_gb) < MAX_DRIVE_SIZE_GB:
            # Immediately update cache with new size
            await update_drive_size_cache(drive, used_space_gb=used_space_gb + file_size_gb)
            logger.info(f"Selected drive {drive} for {file_size_gb:.2f}GB file")
            return drive
    
    # If no suitable drive in cache, check each drive's actual size
    for drive in DRIVES:
        try:
            used_space_gb = await get_drive_size(drive)
            if used_space_gb is not None:
                # Update cache with actual size
                await update_drive_size_cache(drive, used_space_gb=used_space_gb)
                
                if (used_space_gb + file_size_gb) < MAX_DRIVE_SIZE_GB:
                    # Immediately update cache with new size including the file
                    await update_drive_size_cache(drive, used_space_gb=used_space_gb + file_size_gb)
                    logger.info(f"Selected drive {drive} for {file_size_gb:.2f}GB file")
                    return drive
        except Exception as e:
            logger.error(f"Error checking drive {drive}: {e}")
    
    logger.error(f"No drive available with sufficient space for {file_size_gb:.2f}GB file")
    raise Exception("No drive available with sufficient space")

async def cleanup_old_files():
    """Delete files older than 24 hours from all drives and update size cache"""
    logger.info(f"Starting cleanup of files older than {FILE_CLEANUP_AGE_HOURS} hours")
    
    deleted_bytes = {drive: 0 for drive in DRIVES}
    files_deleted = {drive: 0 for drive in DRIVES}
    current_time = datetime.now(timezone.utc)
    
    for drive in DRIVES:
        try:
            list_cmd = ['rclone', 'lsjson', f'{drive}:', '--files-only', '--recursive']
            stdout, stderr, returncode = await run_subprocess(list_cmd)
            
            if returncode == 0:
                files = json.loads(stdout)
                total_bytes = sum(file.get('Size', 0) for file in files)
                total_gb = bytes_to_gb(total_bytes)
                
                for file in files:
                    try:
                        mod_time = datetime.fromisoformat(file['ModTime'].replace('Z', '+00:00'))
                        time_diff = (current_time - mod_time).total_seconds() / 3600
                        
                        if time_diff > FILE_CLEANUP_AGE_HOURS:
                            file_size = file.get('Size', 0)
                            deleted_bytes[drive] += file_size
                            files_deleted[drive] += 1
                            
                            delete_cmd = [
                                'rclone', 'deletefile',
                                f'{drive}:{file["Path"]}',
                                '--drive-use-trash=false'
                            ]
                            await run_subprocess(delete_cmd)
                    except (ValueError, KeyError) as e:
                        logger.error(f"Error processing file timestamp for {file.get('Path', 'unknown file')}: {e}")
                
                # Update cache with new size information
                deleted_gb = bytes_to_gb(deleted_bytes[drive])
                new_size = total_gb - deleted_gb if deleted_bytes[drive] > 0 else total_gb
                
                # Only update if files were deleted or significant difference
                cache = await read_drive_size_cache()
                old_size = cache.get(drive, {}).get('used_space_gb', 0)
                
                if deleted_bytes[drive] > 0 or abs(old_size - total_gb) > 0.01:
                    await update_drive_size_cache(drive, new_size)
            else:
                logger.error(f"Error listing files on drive {drive}: {stderr}")
        except Exception as e:
            logger.error(f"Error cleaning up drive {drive}: {e}")
    
    # Log summary
    total_deleted_gb = bytes_to_gb(sum(deleted_bytes.values()))
    total_files_deleted = sum(files_deleted.values())
    if total_files_deleted > 0:
        logger.info(f"Cleanup complete: Deleted {total_deleted_gb:.2f}GB across {total_files_deleted} files")

def get_isolated_download_path(identifier):
    """Create and return an isolated download directory for a specific download."""
    download_dir = os.path.join(DOWNLOAD_BASE_DIR, identifier)
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    return download_dir

def cleanup_download_dir(download_dir):
    """Clean up the download directory after use."""
    try:
        if os.path.exists(download_dir):
            shutil.rmtree(download_dir, ignore_errors=True)
    except Exception as e:
        logger.error(f"Error cleaning up directory {download_dir}: {e}")

def store_content_info(identifier, info):
    """Store content info in storage file with timestamp."""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        
        try:
            with open(CONTENT_STORAGE_PATH, 'r', encoding='utf-8') as f:
                storage = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            storage = {}
            
        # Convert info to a serializable format if it's a Task
        if isinstance(info, asyncio.Task):
            try:
                info = info.result()
            except:
                info = {}

        # Add timestamp and store info
        info['timestamp'] = time.time()
        storage[identifier] = info
        
        # Remove entries older than 60 minutes
        current_time = time.time()
        storage = {k:v for k,v in storage.items() 
                  if current_time - v.get('timestamp',0) < 3600}
        
        with open(CONTENT_STORAGE_PATH, 'w', encoding='utf-8') as f:
            json.dump(storage, f, indent=4)
            
        return True
    except Exception as e:
        logger.error(f"Error storing content info: {str(e)}")
        raise

def get_drive_config(drive):
    """Get the rclone config file and drive name for a specific drive."""
    if drive not in DRIVE_CONFIG_MAP:
        logger.error(f"Invalid drive name: {drive}")
        return None, None
        
    config_file = os.path.join(RCLONE_CONFIG_DIR, DRIVE_CONFIG_MAP[drive]["config"])
    drive_name = DRIVE_CONFIG_MAP[drive]["drive_name"]
    
    return config_file, drive_name
