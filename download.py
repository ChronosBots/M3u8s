
import asyncio
import glob
import os
import re
import logging
import time
import shutil
import json

# Get logger
logger = logging.getLogger(__name__)

# These will be imported at runtime from the main module
download_progress = None
progress_display = None

# Import constants
from hotstar import mpd_hotstar_headers
from config import USE_PROXY, MP4_USER_IDS, PROXY_URL, DUMP_STREAMS

class BaseDownloader:
    """Base class for downloaders with common functionality."""
    def __init__(self, stream_url, selected_resolution, selected_audios, content_info, download_dir, filename, identifier):
        self.stream_url = stream_url
        self.selected_resolution = selected_resolution
        self.selected_audios = selected_audios
        self.content_info = content_info
        self.download_dir = download_dir
        self.filename = filename
        self.identifier = identifier
        self.processes = []
        self.progress_data = None
        self.enable_logging = True
        self.needs_decryption = content_info.get("drm", {}).get("needs_decryption", False)
        self.final_merged_path = None
        self.last_progress_update_time = 0

    async def _merge_streams(self, video_path, audio_paths, output_path):
        """Merge downloaded streams using ffmpeg."""
        input_args = ['-i', video_path]
        map_args = ['-map', '0:v']

        # Handle audio paths if they exist
        if audio_paths:
            for i, audio_path in enumerate(audio_paths):
                input_args.extend(['-i', audio_path])
                map_args.extend(['-map', f'{i+1}:a'])
        else:
            # If no audio paths, map all audio streams from video file
            map_args.extend(['-map', '0:a'])
        
        # Map all subtitle streams
        map_args.extend(['-map', '0:s?'])

        # Get user_id to determine if we should use MP4 format
        user_id = self.identifier.split('_')[0] if '_' in self.identifier else None
        use_mp4 = user_id in MP4_USER_IDS
        
        cmd = [
            'ffmpeg', '-y',
            *input_args,
            *map_args,
            '-c', 'copy',
        ]
        
        # Add specific format options if using MP4
        if use_mp4 and output_path.endswith('.mp4'):
            cmd.extend(['-f', 'mp4'])
        
        cmd.append(output_path)

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await process.communicate()

        if process.returncode != 0:
            raise Exception(f"FFmpeg merge failed with return code {process.returncode}")

    async def _cleanup(self, files):
        """Clean up temporary files."""
        logger.info("Cleaning up temporary files...")
        for f in files:
            try:
                if os.path.exists(f):
                    os.remove(f)
                    logger.info(f"Removed temporary file: {f}")
            except Exception as e:
                logger.error(f"Error cleaning up {f}: {e}")

    async def _find_files(self, pattern_list):
        """Find files matching any of the given patterns."""
        found_files = []
        for pattern in pattern_list:
            found_files.extend(glob.glob(pattern))
        return found_files

    async def _check_and_delete_existing_files(self):
        """Check and delete any existing video or audio files with the same name"""
        try:
            # Check for existing files with similar names
            base_path = os.path.join(self.download_dir, self.filename)
            
            # Common video and audio extensions
            extensions = ['.mp4', '.mkv', '.m4a', '.aac', '.mp3', '.video', '.audio']
            
            patterns = []
            for ext in extensions:
                # Check exact filename
                patterns.append(base_path + ext)
                # Check for files with copy in name
                patterns.append(base_path + '*copy*' + ext)
                # Check language-specific files
                patterns.append(base_path + '.*' + ext)
            
            existing_files = await self._find_files(patterns)

            # Delete found files
            for file in existing_files:
                try:
                    os.remove(file)
                    logger.info(f"Deleted existing file: {file}")
                except Exception as e:
                    logger.error(f"Error deleting file {file}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error checking/deleting existing files: {str(e)}")

    def _init_progress_data(self):
        """Initialize progress data structure"""
        return {
            'video': {
                'resolution': self.selected_resolution.get('resolution', 'N/A'),
                'bitrate': self.selected_resolution.get('bitrate', 0),
                'type': 'Main',
                'fragments': 0,
                'total_fragments': 0,
                'percentage': 0,
                'downloaded_size': '0MB',
                'total_size': '0MB',
                'speed': '0 KBps',
                'eta': '00:00'
            },
            'audio': {},
            'status': 'Download',
            'platform': self.content_info.get('platform', 'Unknown'),
            'filename': self.filename
        }
        
    async def _get_selected_audio_streams(self):
        """Get selected audio streams from content info."""
        return [
            stream for stream in self.content_info.get("streams_info", {}).get("audio", [])
            if stream["stream_id"] in self.selected_audios
        ]
    
    async def _get_audio_language_suffixes(self, selected_audio_streams):
        language_counts = {}
        audio_language_info = []
        
        for idx, (audio_id, audio_stream) in enumerate(zip(self.selected_audios, selected_audio_streams), 1):
            # Get language or use a default if not available
            language = audio_stream.get("language", f"audio{idx}")
            
            # Update language count and append number if needed
            if language in language_counts:
                language_counts[language] += 1
                language_suffix = f"{language}{language_counts[language]}"
            else:
                language_counts[language] = 0
                language_suffix = language
                
            audio_language_info.append((audio_id, language_suffix))
            
        return audio_language_info
        
    async def _create_final_output_file(self, video_file, audio_files):
        """Create the final output file by merging video and audio."""
        logger.info("Starting merge process...")
        # Get user_id from identifier
        user_id = self.identifier.split('_')[0] if '_' in self.identifier else None
        extension = "mp4" if user_id in MP4_USER_IDS else "mkv"
        final_file = os.path.join(self.download_dir, f"{self.filename}.{extension}")
        try:
            # If no audio files were found, just copy/rename the video file
            if not audio_files:
                logger.warning("No audio files found, copying video only")
                await self._merge_streams(video_file, [], final_file)
            else:
                # Merge with audio files
                await self._merge_streams(video_file, audio_files, final_file)
            if not os.path.exists(final_file) or os.path.getsize(final_file) == 0:
                logger.error("Merged file missing or empty")
                return None
            self.final_merged_path = final_file
            # Record stream files after muxing
            await self._record_stream_files(video_file, audio_files)
            return final_file
        except Exception as e:
            logger.error(f"Merge failed: {e}")
            return None

    async def _record_stream_files(self, video_file, audio_files):
        """Move the video/audio stream files to data/dumps/ and record their new paths in a JSON file with stream_id, content_id, and platform. Do not dump files <1MB or if needs_decryption is True and keys is empty."""
        try:
            # If DUMP_STREAMS is False, delete files and return
            if not DUMP_STREAMS:
                try:
                    if os.path.exists(video_file):
                        os.remove(video_file)
                    for audio_file in audio_files:
                        if os.path.exists(audio_file):
                            os.remove(audio_file)
                except Exception as e:
                    logger.error(f"Failed to delete files after muxing: {e}")
                return
                
            content_id = self.content_info.get("content_id") or self.content_info.get("contentId") or self.content_info.get("id")
            platform = self.content_info.get("platform")
            drm = self.content_info.get("drm", {})
            needs_decryption = drm.get("needs_decryption", False)
            keys = drm.get("keys")
            if not content_id:
                logger.warning("No content_id found in content_info, skipping stream record.")
                return
            record_path = os.path.join("data", "stream_records.json")
            dumps_dir = os.path.join("data", "dumps")
            os.makedirs(dumps_dir, exist_ok=True)
            os.makedirs("data", exist_ok=True)
            try:
                with open(record_path, "r", encoding="utf-8") as f:
                    records = json.load(f)
            except Exception:
                records = []
            video_stream_id = self.selected_resolution.get("stream_id")
            # Video: only dump if >=1MB and not (needs_decryption True and keys empty)
            if os.path.exists(video_file):
                skip_dump = needs_decryption and (not keys or (isinstance(keys, str) and not keys.strip()))
                if skip_dump:
                    try:
                        os.remove(video_file)
                    except Exception:
                        pass
                elif os.path.getsize(video_file) >= 1024 * 1024:
                    video_new_path = os.path.join(dumps_dir, os.path.basename(video_file))
                    if os.path.abspath(video_file) != os.path.abspath(video_new_path):
                        try:
                            shutil.move(video_file, video_new_path)
                        except Exception as e:
                            logger.error(f"Failed to move video file: {e}")
                            video_new_path = video_file
                    if video_stream_id:
                        records.append({
                            "content_id": content_id,
                            "stream_id": video_stream_id,
                            "file_path": video_new_path,
                            "type": "video",
                            "platform": platform,
                            "timestamp": time.time()  # Add timestamp
                        })
                else:
                    try:
                        os.remove(video_file)
                    except Exception:
                        pass
            selected_audio_streams = await self._get_selected_audio_streams()
            for idx, audio_file in enumerate(audio_files):
                if os.path.exists(audio_file):
                    skip_dump = needs_decryption and (not keys or (isinstance(keys, str) and not keys.strip()))
                    if skip_dump:
                        try:
                            os.remove(audio_file)
                        except Exception:
                            pass
                    elif os.path.getsize(audio_file) >= 1024 * 1024:
                        audio_new_path = os.path.join(dumps_dir, os.path.basename(audio_file))
                        if os.path.abspath(audio_file) != os.path.abspath(audio_new_path):
                            try:
                                shutil.move(audio_file, audio_new_path)
                            except Exception as e:
                                logger.error(f"Failed to move audio file: {e}")
                                audio_new_path = audio_file
                        if idx < len(self.selected_audios):
                            audio_stream_id = self.selected_audios[idx]
                            records.append({
                                "content_id": content_id,
                                "stream_id": audio_stream_id,
                                "file_path": audio_new_path,
                                "type": "audio",
                                "platform": platform,
                                "timestamp": time.time()  # Add timestamp
                            })
                    else:
                        try:
                            os.remove(audio_file)
                        except Exception:
                            pass
            with open(record_path, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2)
            
            # Mark download as complete in progress JSON
            await self._update_progress_json(force=True)
                
        except Exception as e:
            logger.error(f"Failed to record stream files: {e}")

    async def execute(self):
        """Base execute method to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement this method")
    async def get_stderr(self):
        """Base get_stderr method to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement this method")

    async def _update_progress_json(self, force=False):
        """Update download progress in a structured JSON file.
        
        Structure:
        {
            "platform_name": {
                "content_id": {
                    "video_stream_id1": {
                        "percentage": 50.5,
                        "download_done": false,
                        "type": "video",
                        "resolution": "1920x1080",
                        "bitrate": 4000,
                        "speed": "1024 KBps",
                        "downloaded_size": "100MB",
                        "total_size": "200MB"
                    },
                    "video_stream_id2": {
                        "percentage": 30.5,
                        "download_done": false,
                        "type": "video",
                        "resolution": "1280x720",
                        "bitrate": 2000,
                        "speed": "512 KBps",
                        "downloaded_size": "50MB",
                        "total_size": "150MB"
                    },
                    "audio_stream_id1": {
                        "percentage": 75.2,
                        "download_done": false,
                        "type": "audio",
                        "language": "english",
                        "speed": "256 KBps",
                        "downloaded_size": "20MB",
                        "total_size": "30MB"
                    },
                    "download_complete": false
                }
            }
        }
        """
        if not self.progress_data:
            return
            
        # Only update every 10 seconds unless force=True
        current_time = time.time()
        if not force and current_time - self.last_progress_update_time < 10:
            return
            
        self.last_progress_update_time = current_time
        
        try:
            platform = self.content_info.get('platform', 'Unknown')
            content_id = self.content_info.get('content_id') or self.content_info.get('contentId') or self.content_info.get('id')
            
            if not content_id:
                logger.warning("No content_id found, skipping progress tracking")
                return
                
            progress_file = os.path.join("data", "download_progress.json")
            os.makedirs("data", exist_ok=True)
            
            # Load existing progress data
            try:
                with open(progress_file, "r", encoding="utf-8") as f:
                    progress_data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                progress_data = {}
                
            # Ensure nested structure exists
            if platform not in progress_data:
                progress_data[platform] = {}
            if content_id not in progress_data[platform]:
                progress_data[platform][content_id] = {"download_complete": False}
                
            # Update video progress
            video_stream_id = self.selected_resolution.get("stream_id")
            if video_stream_id:
                progress_data[platform][content_id][video_stream_id] = {
                    "percentage": self.progress_data['video'].get('percentage', 0),
                    "download_done": self.progress_data['video'].get('percentage', 0) >= 100,
                    "type": "video",
                    "resolution": self.selected_resolution.get("resolution", "N/A"),
                    "bitrate": self.selected_resolution.get("bitrate", 0),
                    "speed": self.progress_data['video'].get('speed', '0 KBps'),
                    "downloaded_size": self.progress_data['video'].get('downloaded_size', '0MB'),
                    "total_size": self.progress_data['video'].get('total_size', '0MB')
                }
                
            # Update audio progress
            for audio_idx, audio_id in enumerate(self.selected_audios):
                # Find the language for this audio stream
                language = None
                for lang, audio_data in self.progress_data.get('audio', {}).items():
                    # Just use the first language we find for this audio stream
                    language = lang
                    percentage = audio_data.get('percentage', 0)
                    progress_data[platform][content_id][audio_id] = {
                        "percentage": percentage,
                        "download_done": percentage >= 100,
                        "type": "audio",
                        "language": language,
                        "speed": audio_data.get('speed', '0 KBps'),
                        "downloaded_size": audio_data.get('downloaded_size', '0MB'),
                        "total_size": audio_data.get('total_size', '0MB')
                    }
                    break
                    
            # Check if everything is complete
            all_complete = True
            for stream_id, stream_data in progress_data[platform][content_id].items():
                if stream_id != "download_complete" and not stream_data.get("download_done", False):
                    all_complete = False
                    break
                    
            progress_data[platform][content_id]["download_complete"] = all_complete
            
            # Write progress data to file
            with open(progress_file, "w", encoding="utf-8") as f:
                json.dump(progress_data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error updating progress JSON: {e}")

# Helper to check for dumped streams
async def get_dumped_stream_file(content_id, stream_id, stream_type, platform=None):
    # Don't use dumped files if DUMP_STREAMS is disabled
    if not DUMP_STREAMS:
        return None
        
    record_path = os.path.join("data", "stream_records.json")
    try:
        with open(record_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        for rec in records:
            fpath = rec.get("file_path", "")
            if (
                rec.get("content_id") == content_id and
                rec.get("stream_id") == stream_id and
                rec.get("type") == stream_type and
                (platform is None or rec.get("platform") == platform) and
                os.path.exists(fpath) and os.path.getsize(fpath) >= 1024 * 1024
            ):
                return fpath
    except Exception:
        pass
    return None

class YTDLPDownloader(BaseDownloader):
    def __init__(self, stream_url, selected_resolution, selected_audios, content_info, download_dir, filename, identifier):
        super().__init__(stream_url, selected_resolution, selected_audios, content_info, download_dir, filename, identifier)

    async def _build_yt_dlp_command(self, format_id, output_file):
        """Build yt-dlp command for individual stream."""
        cmd = [
            'yt-dlp',
            '-f', format_id,
            '--output', output_file,
            '--concurrent-fragments', '150',
            '--geo-bypass-country', 'IN',
            '--allow-unplayable-formats',
            '--no-part',
            '--retries', 'infinite',
            '--fragment-retries', 'infinite',
            '--file-access-retries', 'infinite',
            '--newline',
            '--progress'
        ]

        # Platform specific headers and proxy
        platform = self.content_info.get("platform")
        
        # Only add proxy if USE_PROXY is True and PROXY_URL is not None
        if USE_PROXY:
            if platform == "JioHotstar" and PROXY_URL:
                cmd.extend(['--proxy', PROXY_URL])
        if platform == "JioHotstar":
            for key, value in mpd_hotstar_headers.items():
                cmd.extend(['--add-header', f'{key}:{value}'])
                
        cmd.append(self.stream_url)
        return cmd

    async def _execute_download(self, format_id, output_file, stream_type):
        """Execute download for a single stream."""
        cmd = await self._build_yt_dlp_command(format_id, output_file)
        logger.info(f"YTDLP Download command: {' '.join(cmd)}")

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        self.processes.append((process, stream_type, output_file))
        return process

    async def _parse_progress_line(self, line, stream_type, selected_audio_streams):
        """Parse a single line of yt-dlp output and update progress_data."""
        line = line.decode().strip()

        if self.enable_logging:  # Only log if enabled
            logger.info(f"[{stream_type}] {line}")

        # Parse video stream info
        if '[download] Destination' in line and stream_type == 'video':
            res_match = re.search(r'(\d{3,4}p)', self.progress_data['video']['resolution'])
            if res_match:
                self.progress_data['video']['resolution'] = res_match.group(1)

        if '[download]' in line:
            try:
                # Extract resolution and bitrate
                vid_info_match = re.search(r'(\d+x\d+).*?(\d+)K', line)
                if vid_info_match:
                    self.progress_data['video']['resolution'] = vid_info_match.group(1)
                    self.progress_data['video']['bitrate'] = int(vid_info_match.group(2))

                # Extract fragments
                frag_match = re.search(r'(\d+)/(\d+)\s+(\d+\.\d+)%', line)
                if frag_match:
                    self.progress_data['video']['fragments'] = int(frag_match.group(1))
                    self.progress_data['video']['total_fragments'] = int(frag_match.group(2))
                    self.progress_data['video']['percentage'] = float(frag_match.group(3))

                # Extract size info
                size_match = re.search(r'([\d.]+)MiB/([\d.]+)MiB', line)
                if size_match:
                    self.progress_data['video']['downloaded_size'] = f"{size_match.group(1)}MB"
                    self.progress_data['video']['total_size'] = f"{size_match.group(2)}MB"

                # Extract speed and ETA
                speed_match = re.search(r'(\d+\.?\d*[KM]iB/s)', line)
                eta_match = re.search(r'ETA (\d+:\d+)', line)

                if speed_match:
                    speed = speed_match.group(1)
                    if 'MiB/s' in speed:
                        mb = float(speed.replace('MiB/s', '')) * 1024
                        self.progress_data['video']['speed'] = f"{mb:.0f} KBps"
                    else:
                        self.progress_data['video']['speed'] = f"{speed.replace('KiB/s', '')} KBps"

                if eta_match:
                    self.progress_data['video']['eta'] = eta_match.group(1)

                if stream_type == 'video' and not frag_match:
                    general_percent = re.search(r'(\d+\.\d+)%', line)
                    if general_percent:
                        self.progress_data['video']['percentage'] = float(general_percent.group(1))

                if stream_type.startswith('audio_'):
                    audio_idx = int(stream_type.split('_')[1]) - 1
                    if 0 <= audio_idx < len(selected_audio_streams):
                        language = selected_audio_streams[audio_idx]["language"]
                        if language not in self.progress_data['audio']:
                            self.progress_data['audio'][language] = {
                                'percentage': 0,
                                'speed': '0 KBps',
                                'downloaded_size': '0MB',
                                'total_size': '0MB'
                            }
                        audio_percent = re.search(r'(\d+\.\d+)%', line)
                        if audio_percent:
                            current = float(audio_percent.group(1))
                            self.progress_data['audio'][language]['percentage'] = 100 if current >= 94 else current

                download_progress.update_progress(self.identifier, self.progress_data)
                await self._update_progress_json()

            except Exception as e:
                logger.error(f"Progress parsing error: {e}")

    async def _monitor_progress(self, process, stream_type, output_file):
        """Monitor download progress for a single stream."""
        # Initialize progress data
        if self.progress_data is None:
            self.progress_data = self._init_progress_data()

        selected_audio_streams = await self._get_selected_audio_streams()

        while True:
            line = await process.stdout.readline()
            if not line:
                break
            await self._parse_progress_line(line, stream_type, selected_audio_streams)

        return await process.wait()

    async def get_stderr(self):
        """Get stderr from all processes."""
        stderr_data = []
        for process, stream_type, _ in self.processes:
            if process.stderr:
                stderr = await process.stderr.read()
                if stderr:
                    stderr_data.append(f"{stream_type}: {stderr.decode()}")
        return "\n".join(stderr_data).encode() if stderr_data else None

    async def _decrypt_file(self, file_path, keys_dict):
        """Attempt to decrypt a single file using all available keys."""
        output_path = file_path + '.decrypted'
        
        cmd = ['mp4decrypt']
        for kid, key in keys_dict.items():
            cmd.extend(['--key', f'{kid}:{key}'])
        cmd.extend([file_path, output_path])
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await process.communicate()

        if process.returncode == 0:
            os.replace(output_path, file_path)
            return True
        else:
            if os.path.exists(output_path):
                os.remove(output_path)
            return False

    async def _decrypt_streams(self, files_to_decrypt):
        """Decrypt downloaded streams if necessary."""
        if not self.content_info or 'drm' not in self.content_info or not self.content_info['drm'].get('keys'):
            return files_to_decrypt

        keys_str = self.content_info['drm']['keys']
        key_pairs = keys_str.split(',') if isinstance(keys_str, str) else keys_str
        keys_dict = {kid.strip(): key.strip() for pair in key_pairs for kid, key in [pair.split(':')]}

        decrypted_files = []
        
        try:
            # Try mp4decrypt first
            for file_path in files_to_decrypt:
                success = await self._decrypt_file(file_path, keys_dict)
                if not success:
                    # Fall back to Shaka Packager if mp4decrypt fails
                    success = await self._decrypt_file_shaka(file_path, keys_dict)
                    if not success:
                        raise Exception(f"Failed to decrypt {file_path}")
                decrypted_files.append(file_path)
            
            return decrypted_files
            
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise

    async def _download_and_monitor(self):
        """Downloads video and audio streams and monitors their progress, using dumps if available."""
        selected_audio_streams = await self._get_selected_audio_streams()
        content_id = self.content_info.get("content_id") or self.content_info.get("contentId") or self.content_info.get("id")

        # Video
        video_stream_id = self.selected_resolution["stream_id"]
        video_file = await get_dumped_stream_file(content_id, video_stream_id, "video", self.content_info.get("platform"))
        if video_file:
            logger.info(f"Using dumped video file: {video_file}")
            # Set video progress to 100%
            if self.progress_data is None:
                self.progress_data = self._init_progress_data()
            self.progress_data['video']['percentage'] = 100
            self.progress_data['video']['downloaded_size'] = self.progress_data['video']['total_size'] = f"{os.path.getsize(video_file) // (1024*1024)}MB"
            download_progress.update_progress(self.identifier, self.progress_data)
        else:
            video_file = os.path.join(self.download_dir, f"{self.filename}.video")
            await self._execute_download(video_stream_id, video_file, 'video')

        # Audio
        audio_language_info = await self._get_audio_language_suffixes(selected_audio_streams)
        audio_files = []
        for idx, (audio_id, language_suffix) in enumerate(audio_language_info, 1):
            audio_file = await get_dumped_stream_file(content_id, audio_id, "audio", self.content_info.get("platform"))
            if audio_file:
                logger.info(f"Using dumped audio file: {audio_file}")
                # Set audio progress to 100%
                if self.progress_data is None:
                    self.progress_data = self._init_progress_data()
                lang = selected_audio_streams[idx-1]["language"] if idx-1 < len(selected_audio_streams) else language_suffix
                if lang not in self.progress_data['audio']:
                    self.progress_data['audio'][lang] = {}
                self.progress_data['audio'][lang]['percentage'] = 100
                self.progress_data['audio'][lang]['downloaded_size'] = self.progress_data['audio'][lang]['total_size'] = f"{os.path.getsize(audio_file) // (1024*1024)}MB"
                download_progress.update_progress(self.identifier, self.progress_data)
            else:
                audio_file = os.path.join(self.download_dir, f"{self.filename}.{language_suffix}")
                await self._execute_download(audio_id, audio_file, f'audio_{idx}')
            audio_files.append(audio_file)

        # Only monitor downloads that were actually started
        tasks = [self._monitor_progress(process, stream_type, output_file) for process, stream_type, output_file in self.processes]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        return video_file, audio_files

    async def _check_downloaded_files(self, video_file, audio_files):
        """Checks if downloaded files exist and are not empty."""
        if not os.path.exists(video_file) or os.path.getsize(video_file) == 0:
            logger.error(f"Video file missing or empty: {video_file}")
            return False

        for audio_file in audio_files:
            if not os.path.exists(audio_file) or os.path.getsize(audio_file) == 0:
                logger.error(f"Audio file missing or empty: {audio_file}")
                return False
        return True

    async def execute(self):
        """Execute the download, decryption, and merge process."""
        try:
            # Check and delete existing files first
            await self._check_and_delete_existing_files()
            
            video_file, audio_files = await self._download_and_monitor()

            if not await self._check_downloaded_files(video_file, audio_files):
                return 1

            if self.needs_decryption:
                files_to_decrypt = [video_file] + audio_files
                try:
                    await self._decrypt_streams(files_to_decrypt)
                except Exception as e:
                    logger.error(f"Decryption failed: {e}")
                    return 1

            final_file = await self._create_final_output_file(video_file, audio_files)
            
            if not final_file:
                return 1
                
            # Update progress JSON with download complete
            if self.progress_data is None:
                self.progress_data = self._init_progress_data()
            self.progress_data['video']['percentage'] = 100
            for lang in self.progress_data.get('audio', {}):
                self.progress_data['audio'][lang]['percentage'] = 100
            download_progress.update_progress(self.identifier, self.progress_data)
            await self._update_progress_json(force=True)
            
            return 0

        except Exception as e:
            logger.error(f"Download failed: {e}")
            return 1
        
    async def _decrypt_file_shaka(self, file_path, keys_dict):
        """Attempt to decrypt a single file using Shaka Packager."""
        output_path = file_path + '.decrypted'
        
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            packager_path = os.path.join(script_dir, 'packager')
            
            if not os.path.exists(packager_path):
                return False
            
            if not os.access(packager_path, os.X_OK):
                os.chmod(packager_path, 0o755)
            
            cmd = [packager_path, '--enable_raw_key_decryption']
            
            is_video = not any(audio_indicator in file_path.lower() 
                             for audio_indicator in ['.aac', '.mp3', '.m4a', '.audio', '.hindi', '.tamil', '.telugu'])
            drm_label = "VIDEO" if is_video else "AUDIO"
            
            key_specs = []
            for idx, (kid, key) in enumerate(keys_dict.items(), 1):
                label = f"{drm_label}{idx if idx > 1 else ''}"
                key_specs.append(f"label={label}:key_id={kid}:key={key}")
            if key_specs:
                cmd.extend(['--keys', ','.join(key_specs)])
            
            input_format = "webm" if file_path.lower().endswith('.webm') else "mp4"
            output_format = input_format
            
            stream_descriptor = (
                f"input={file_path},"
                f"stream_selector=0,"
                f"drm_label={drm_label},"
                f"output={output_path},"
                f"input_format={input_format},"
                f"output_format={output_format}"
            )
            
            cmd.append(stream_descriptor)
            
            logger.info("Running Shaka Packager command:")
            logger.info(f"Command: {' '.join(cmd)}")
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=os.environ.copy()
            )
            await process.communicate()
            
            if process.returncode == 0:
                os.replace(output_path, file_path)
                return True
            else:
                if os.path.exists(output_path):
                    os.remove(output_path)
                return False
                
        except FileNotFoundError:
            return False
        except Exception:
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except:
                    pass
            return False

class Nm3u8DLREDownloader(BaseDownloader):
    def __init__(self, stream_url, selected_resolution, selected_audios, content_info, download_dir, filename, identifier, selected_codec=None):
        super().__init__(stream_url, selected_resolution, selected_audios, content_info, download_dir, filename, identifier)
        self.selected_codec = selected_codec

    async def _build_common_command_parts(self, stream_type, stream_id=None, language_suffix=None):
        """Build common parts of N_m3u8DL-RE command"""
        platform = self.content_info.get("platform")
        
        # Base command with stream URL
        cmd_parts = [f"N_m3u8DL-RE '{self.stream_url}'"]
        
        # Check if user wants MP4 format
        user_id = self.identifier.split('_')[0] if '_' in self.identifier else None
        use_mp4 = user_id in MP4_USER_IDS
        
        # Stream selection based on type
        if stream_type == "video":
            selection_parts = []

            # Always include resolution as a filter. Use flexible matching.
            try:
                height = self.selected_resolution['resolution'].split('x')[1]
                selection_parts.append(f'res<={height}')
            except (KeyError, IndexError):
                logger.warning("Could not parse height from resolution, using exact match.")
                selection_parts.append(f'res={self.selected_resolution["resolution"]}')

            # Apply codec filter if selected.
            if self.selected_codec:
                if self.selected_codec.lower() == 'h265':
                    codec_pattern = 'hvc|hev'
                elif self.selected_codec.lower() == 'h264':
                    codec_pattern = 'avc'
                else:
                    codec_pattern = self.selected_codec.lower()
                selection_parts.append(f'codec~={codec_pattern}')
            else:
                # Fallback to stream ID if no codec is selected.
                selection_parts.append(f'id={self.selected_resolution["stream_id"]}')
            
            # Combine filters with '+' for AND logic
            selection_string = '+'.join(selection_parts)
            
            # Use ':for=best' to get the highest quality stream matching the criteria
            video_param = f'-sv "{selection_string}:for=best"'
            
            cmd_parts.append(video_param)
            
            # Include subtitle selection for video streams
            subtitle_param = '-ss "all"'
            cmd_parts.append(subtitle_param)
            
            cmd_parts.append('-da "all"')
            
            # Add concurrent download flag
            cmd_parts.append('-mt')
            
            # Add mux-after-done option with proper format
            mux_format = "mp4" if use_mp4 else "mkv"
            cmd_parts.append(f'-M "format={mux_format}"')
            
            save_name = f"{self.filename}.video"
            
        elif stream_type == "audio":
            # Audio stream selection
            if stream_id:
                # Use only the specific audio ID for this command, not all selected audios
                audio_param = f'-sa "id={stream_id}:for=best"'
            else:
                # Fallback, though this shouldn't happen normally
                audio_param = '-sa "best"'
            cmd_parts.append(audio_param)
            cmd_parts.extend(['-dv "all"', '-ds "all"'])
            save_name = f"{self.filename}.{language_suffix}"
        
        # Output parameters
        cmd_parts.extend([
            '--thread-count 40',
            '--skip-merge false',
            '--del-after-done false',
            '--write-meta-json false',
            f'--save-dir "{self.download_dir}"',
            f'--save-name "{save_name}"'
        ])
        # Add proxy if needed
        if USE_PROXY and platform == "JioHotstar" and PROXY_URL:
            cmd_parts.append(f'--custom-proxy "{PROXY_URL}"')
        
        # Add platform-specific headers
        if platform == "JioHotstar":
            for key, value in mpd_hotstar_headers.items():
                cmd_parts.append(f'-H "{key}: {value}"')
        
        # Add DRM keys if needed
        if self.content_info.get("drm", {}).get("needs_decryption") and self.content_info.get("drm", {}).get("keys"):
            keys = self.content_info["drm"]["keys"]
            key_pairs = keys.split(",") if isinstance(keys, str) else keys
            cmd_parts.extend('--key "{}"'.format(key.strip()) for key in key_pairs)
            
        return cmd_parts

    async def build_video_command(self):
        """Build the N_m3u8DL-RE command for video download"""
        cmd_parts = await self._build_common_command_parts("video")
        return " ".join(cmd_parts)
        
    async def build_audio_command(self, audio_id, language_suffix):
        """Build the N_m3u8DL-RE command for audio download"""
        cmd_parts = await self._build_common_command_parts("audio", audio_id, language_suffix)
        return " ".join(cmd_parts)
    
    async def _execute_download(self, cmd, stream_type):
        """Execute download for a single stream."""
        logger.info(f"[{stream_type}] Download command: {cmd}")

        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            shell=True
        )
        self.processes.append((process, stream_type))
        return process

    async def _monitor_progress(self, process, stream_type):
        """Monitor download progress for a single stream."""
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            line = line.decode().strip()
            if line:
                if self.enable_logging:  # Only log if enabled
                    logger.info(f"[{stream_type}] {line}")
                try:
                    if self.progress_data:
                        self.progress_data = await progress_display.update_progress_from_line(
                            line, self.progress_data, self.identifier
                        )
                        download_progress.update_progress(self.identifier, self.progress_data)
                        await self._update_progress_json()
                except Exception as e:
                    logger.error(f"Error updating progress for {self.identifier}: {e}")

        return await process.wait()

    async def get_stderr(self):
        """Get any remaining stderr output from all processes"""
        stderr_data = []
        for process, stream_type in self.processes:
            if process.stderr:
                stderr = await process.stderr.read()
                if stderr:
                    stderr_data.append(f"{stream_type}: {stderr.decode()}")
        return "\n".join(stderr_data).encode() if stderr_data else None

    async def execute(self):
        """Execute the download process and monitor progress, using dumps if available."""
        try:
            await self._check_and_delete_existing_files()
            self.progress_data = self._init_progress_data()
            download_progress.update_progress(self.identifier, self.progress_data)
            
            selected_audio_streams = await self._get_selected_audio_streams()
            audio_language_info = await self._get_audio_language_suffixes(selected_audio_streams)
            audio_track_info = []
            content_id = self.content_info.get("content_id") or self.content_info.get("contentId") or self.content_info.get("id")

            # Video
            video_stream_id = self.selected_resolution["stream_id"]
            video_file = await get_dumped_stream_file(content_id, video_stream_id, "video", self.content_info.get("platform"))
            if video_file:
                logger.info(f"Using dumped video file: {video_file}")
                # Set video progress to 100%
                self.progress_data['video']['percentage'] = 100
                self.progress_data['video']['downloaded_size'] = self.progress_data['video']['total_size'] = f"{os.path.getsize(video_file) // (1024*1024)}MB"
                download_progress.update_progress(self.identifier, self.progress_data)
            else:
                video_cmd = await self.build_video_command()
                video_file = os.path.join(self.download_dir, f"{self.filename}.video")
                video_process = await self._execute_download(video_cmd, 'video')

            # Audio
            audio_files = []
            for idx, (audio_id, language_suffix) in enumerate(audio_language_info, 1):
                audio_file = await get_dumped_stream_file(content_id, audio_id, "audio", self.content_info.get("platform"))
                if audio_file:
                    logger.info(f"Using dumped audio file: {audio_file}")
                    # Set audio progress to 100%
                    lang = selected_audio_streams[idx-1]["language"] if idx-1 < len(selected_audio_streams) else language_suffix
                    if lang not in self.progress_data['audio']:
                        self.progress_data['audio'][lang] = {}
                    self.progress_data['audio'][lang]['percentage'] = 100
                    self.progress_data['audio'][lang]['downloaded_size'] = self.progress_data['audio'][lang]['total_size'] = f"{os.path.getsize(audio_file) // (1024*1024)}MB"
                    download_progress.update_progress(self.identifier, self.progress_data)
                else:
                    audio_cmd = await self.build_audio_command(audio_id, language_suffix)
                    audio_file = os.path.join(self.download_dir, f"{self.filename}.{language_suffix}")
                    await self._execute_download(audio_cmd, f'audio_{idx}')
                audio_files.append(audio_file)
                audio_track_info.append((language_suffix, audio_id))

            # Only monitor downloads that were actually started
            monitoring_tasks = [self._monitor_progress(process, stream_type) for process, stream_type in self.processes]
            if monitoring_tasks:
                await asyncio.gather(*monitoring_tasks)

            # Find the video file (if it was downloaded, it will be in the download dir, else it's from dump)
            if not os.path.exists(video_file):
                # Try to find it by pattern (for downloaded case)
                video_patterns = [
                    os.path.join(self.download_dir, f"{self.filename}.video.*"),
                    os.path.join(self.download_dir, f"{self.filename}.*.[mw][kp][v4]")
                ]
                res = self.selected_resolution.get("resolution", "")
                if res:
                    res_pattern = res.split("x")[1] + "p" if "x" in res else res
                    video_patterns.append(os.path.join(self.download_dir, f"*{res_pattern}*.[mw][kp][v4]"))
                video_files = await self._find_files(video_patterns)
                if video_files:
                    video_files.sort(key=lambda x: os.path.getsize(x), reverse=True)
                    video_file = video_files[0]
                    logger.info(f"Found video file: {video_file}")
                else:
                    logger.error("No video file found in download directory")
                    return 1

            # Find audio files (if not from dump)
            final_audio_files = []
            for idx, (language_suffix, audio_id) in enumerate(audio_track_info):
                audio_file = audio_files[idx]
                if not os.path.exists(audio_file):
                    # Try to find it by pattern
                    audio_patterns = [
                        os.path.join(self.download_dir, f"{self.filename}.{language_suffix}.*"),
                        os.path.join(self.download_dir, f"*part{language_suffix}*.*"),
                        os.path.join(self.download_dir, f"*{language_suffix}*.m4a"),
                        os.path.join(self.download_dir, f"*{language_suffix}*.aac"),
                        os.path.join(self.download_dir, f"*{audio_id}*.*")
                    ]
                    found_audio_files = await self._find_files(audio_patterns)
                    audio_matches = [f for f in found_audio_files if not f.endswith(('.mp4', '.mkv', '.webm', '.srt', '.vtt'))]
                    if audio_matches:
                        audio_file = audio_matches[0]
                        logger.info(f"Found audio file for {language_suffix}: {audio_file}")
                final_audio_files.append(audio_file)

            # Deduplicate audio files while preserving order
            seen_files = set()
            ordered_audio_files = []
            for file in final_audio_files:
                if file not in seen_files:
                    seen_files.add(file)
                    ordered_audio_files.append(file)

            logger.info(f"Video file for merging: {video_file}")
            logger.info(f"Audio files for merging (in order): {ordered_audio_files}")

            final_file = await self._create_final_output_file(video_file, ordered_audio_files)
            if not final_file:
                return 1
                
            # Update progress JSON with download complete
            self.progress_data['video']['percentage'] = 100
            for lang in self.progress_data.get('audio', {}):
                self.progress_data['audio'][lang]['percentage'] = 100
            download_progress.update_progress(self.identifier, self.progress_data)
            await self._update_progress_json(force=True)
            
            await self._record_stream_files(video_file, ordered_audio_files)
            return 0
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return 1

    async def get_stderr(self):
        """Get stderr from all processes."""
        stderr_data = []
        for process, stream_type in self.processes:
            if process.stderr:
                stderr = await process.stderr.read()
                if stderr:
                    stderr_data.append(f"{stream_type}: {stderr.decode()}")
        return "\n".join(stderr_data).encode() if stderr_data else None

async def periodic_dump_cleanup():
    while True:
        try:
            # Skip cleanup if dumping is disabled
            if not DUMP_STREAMS:
                await asyncio.sleep(3600)  # Sleep for an hour and check again
                continue
                
            record_path = os.path.join("data", "stream_records.json")
            dumps_dir = os.path.join("data", "dumps")
            now = time.time()
            cutoff = now - 48 * 3600  # 48 hours in seconds
            changed = False
            try:
                with open(record_path, "r", encoding="utf-8") as f:
                    records = json.load(f)
            except Exception:
                records = []
            new_records = []
            for rec in records:
                fpath = rec.get("file_path")
                timestamp = rec.get("timestamp")
                
                # Skip records without timestamp (old records) or missing files
                if not timestamp or not fpath or not os.path.exists(fpath):
                    changed = True
                    continue
                
                # Check if file is older than 48 hours
                if timestamp < cutoff:
                    try:
                        os.remove(fpath)
                        changed = True
                        continue  # skip adding to new_records
                    except Exception as e:
                        logger.error(f"Error removing old file {fpath}: {e}")
                        # Keep the record if file couldn't be deleted
                        new_records.append(rec)
                else:
                    new_records.append(rec)
            
            if changed:
                with open(record_path, "w", encoding="utf-8") as f:
                    json.dump(new_records, f, indent=2)
        except Exception as e:
            logger.error(f"Error in periodic_dump_cleanup: {e}")
        await asyncio.sleep(20 * 60)  # 20 minutes
