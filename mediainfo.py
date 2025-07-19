import os
import asyncio
import json
import logging
from telegraph import Telegraph
from urllib.parse import quote

logger = logging.getLogger(__name__)

pickFormats = {
    "audio": {
        'tam': "Tamil", 'tel': "Telugu", 'mal': "Malayalam", 'hin': "Hindi",
        'kan': "Kannada", 'mar': "Marathi", 'ben': "Bengali", 'pun': "Punjabi", 
        'guj': "Gujarati", 'ori': "Odia", 'ass': "Assamese", 'kha': "Kashmiri",
        'sar': "Sanskrit", 'ur': "Urdu", 'ma': "Maithili", 'bho': "Bhojpuri",
        'nep': "Nepali", 'sindhi': "Sindhi", 'santali': "Santali", 'dogri': "Dogri",
        'raj': "Rajasthani", 'eng': "English", 'spa': "Spanish", 'fra': "French",
        'ger': "German", 'chi': "Chinese", 'ja': "Japanese", 'ko': "Korean",
        'en': "English", 'bn': "Bengali", 'gu': "Gujarati", 'kn': "Kannada",
        'mr': "Marathi", 'ml': "Malayalam", 'ta': "Tamil", 'te': "Telugu",
        'hi': "Hindi"
    }
}

# Initialize Telegraph
telegraph = Telegraph()
# Note: You might want to create a persistent token and store it
try:
    telegraph.create_account(short_name='MediaInfoBot', author_name="ⓘⓖⓝⓘⓣⓔ", author_url="https://t.me/Ignitedlbot")
except Exception as e:
    logger.error(f"Error creating Telegraph account: {e}")

async def get_media_info(file_path):
    """Extract media information from a video file using mediainfo command line tool."""
    try:
        # Run mediainfo command with JSON output format
        process = await asyncio.create_subprocess_exec(
            'mediainfo', '--Output=JSON', file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            logger.error(f"MediaInfo command failed: {stderr.decode()}")
            return None
            
        # Parse JSON output
        return json.loads(stdout.decode())
    except Exception as e:
        logger.error(f"Error getting media info: {e}")
        return None

def get_formatted_size(size_bytes):
    """Convert size in bytes to human-readable format."""
    if not size_bytes:
        return "Unknown"
    
    size_bytes = int(size_bytes)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0 or unit == 'GB':
            break
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} {unit}"

def get_formatted_duration(duration_str):
    """Format duration into a readable format."""
    if not duration_str:
        return "Unknown"
    
    try:
        duration = float(duration_str)
        hours = int(duration // 3600)
        minutes = int((duration % 3600) // 60)
        seconds = int(duration % 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    except:
        return duration_str

def format_media_info_telegraph(media_info, filename):
    """Format mediainfo JSON into Telegraph content format with modern card style."""
    if not media_info or 'media' not in media_info:
        return [{"tag": "p", "children": ["Failed to retrieve media information."]}]
    
    # Initialize content array
    content = []
    
    # Add filename with emoji and decoration
    content.append({
        "tag": "h4", 
        "children": [f"📌 {filename}"]
    })
    
    # Add decorative separator
    content.append({
        "tag": "p",
        "children": ["━━━━━━━━━━━━━━━━━━━━━━"]
    })
    
    # Get tracks
    tracks = media_info.get('media', {}).get('track', [])
    
    # Extract general info
    general_track = next((t for t in tracks if t.get('@type') == 'General'), {})
    video_track = next((t for t in tracks if t.get('@type') == 'Video'), {})
    audio_tracks = [t for t in tracks if t.get('@type') == 'Audio']
    subtitle_tracks = [t for t in tracks if t.get('@type') == 'Text']
    
    # OVERVIEW CARD
    content.append({
        "tag": "h4", 
        "children": ["🎬 OVERVIEW"]
    })
    
    overview_items = []
    
    # Size
    file_size = general_track.get('FileSize')
    if file_size:
        size_formatted = get_formatted_size(file_size)
        overview_items.append({
            "tag": "li",
            "children": [
                {"tag": "b", "children": ["Size: "]},
                size_formatted
            ]
        })
    
    # Duration
    duration = general_track.get('Duration')
    if duration:
        duration_formatted = get_formatted_duration(duration)
        overview_items.append({
            "tag": "li",
            "children": [
                {"tag": "b", "children": ["Duration: "]},
                duration_formatted
            ]
        })
    
    # Format
    format_name = general_track.get('Format')
    if format_name:
        overview_items.append({
            "tag": "li",
            "children": [
                {"tag": "b", "children": ["Format: "]},
                format_name
            ]
        })
    
    # Resolution
    if video_track:
        width = video_track.get('Width', '')
        height = video_track.get('Height', '')
        if width and height:
            overview_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Resolution: "]},
                    f"{width}x{height}"
                ]
            })
    
    # Audio info summary
    if audio_tracks:
        audio_track = audio_tracks[0]
        audio_format = audio_track.get('Format', '')
        channels = audio_track.get('Channels', '')
        language_code = audio_track.get('Language', '')
        
        # Convert language code to full name if available
        language = language_code
        if language_code and language_code.lower() in pickFormats['audio']:
            language = pickFormats['audio'][language_code.lower()]
        
        audio_summary = []
        if audio_format:
            audio_summary.append(audio_format)
        if channels:
            audio_summary.append(f"{channels}.0")
        if language:
            audio_summary.append(f"[{language}]")
        
        if audio_summary:
            overview_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Audio: "]},
                    " ".join(audio_summary)
                ]
            })
    
    # Add overview items to content
    if overview_items:
        content.append({
            "tag": "ul",
            "children": overview_items
        })
    
    # Add decorative separator
    content.append({
        "tag": "p",
        "children": ["⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯"]
    })
    
    # VIDEO CARD
    if video_track:
        content.append({
            "tag": "h4", 
            "children": ["📊 VIDEO"]
        })
        
        video_items = []
        
        # Codec
        codec = video_track.get('Format', '')
        if codec:
            video_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Codec: "]},
                    codec
                ]
            })
        
        # Bitrate
        bitrate = video_track.get('BitRate', '')
        if bitrate:
            try:
                bitrate_formatted = f"{int(bitrate) // 1000} kb/s"
            except:
                bitrate_formatted = bitrate
            video_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Bitrate: "]},
                    bitrate_formatted
                ]
            })
        
        # Bitrate Mode
        bitrate_mode = video_track.get('BitRate_Mode', '')
        if bitrate_mode:
            video_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Bitrate Mode: "]},
                    bitrate_mode
                ]
            })
        
        # Framerate
        framerate = video_track.get('FrameRate', '')
        if framerate:
            video_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Framerate: "]},
                    f"{framerate} fps"
                ]
            })
        
        # Profile
        profile = video_track.get('Format_Profile', '')
        if profile:
            video_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Profile: "]},
                    profile
                ]
            })
        
        # Add video items to content
        if video_items:
            content.append({
                "tag": "ul",
                "children": video_items
            })
        
        # Add decorative separator
        content.append({
            "tag": "p",
            "children": ["⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯"]
        })
    
    # AUDIO CARD(S)
    for idx, audio_track in enumerate(audio_tracks):
        title = audio_track.get('Title', '')
        language_code = audio_track.get('Language', '')
        
        # Convert language code to full name if available
        language = language_code
        if language_code and language_code.lower() in pickFormats['audio']:
            language = pickFormats['audio'][language_code.lower()]
        
        section_title = "🔈 AUDIO"
        if title or language:
            extras = []
            if title:
                extras.append(title)
            if language:
                extras.append(language)
            if extras:
                section_title += f" - {', '.join(extras)}"
        
        content.append({
            "tag": "h4", 
            "children": [section_title]
        })
        
        audio_items = []
        
        # Format
        format_name = audio_track.get('Format', '')
        if format_name:
            if "AAC" in format_name:
                format_name = "AAC LC"
            audio_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Format: "]},
                    format_name
                ]
            })
        
        # Commercial Format Name (e.g., Dolby Digital Plus)
        format_commercial = audio_track.get('Format_Commercial_IfAny', '')
        if format_commercial:
            audio_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Commercial Format: "]},
                    format_commercial
                ]
            })
        
        # Channels
        channels = audio_track.get('Channels', '')
        if channels:
            channels_text = f"{channels}.0"
            audio_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Channels: "]},
                    channels_text
                ]
            })
        
        # Language
        if language:
            audio_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Language: "]},
                    language
                ]
            })
        
        # Bitrate
        bitrate = audio_track.get('BitRate', '')
        if bitrate:
            try:
                bitrate_formatted = f"{int(bitrate) // 1000} kb/s"
            except:
                bitrate_formatted = bitrate
            audio_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Bitrate: "]},
                    bitrate_formatted
                ]
            })
        
        # Bitrate Mode
        bitrate_mode = audio_track.get('BitRate_Mode', '')
        if bitrate_mode:
            audio_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Bitrate Mode: "]},
                    bitrate_mode
                ]
            })
        
        # Add audio items to content
        if audio_items:
            content.append({
                "tag": "ul",
                "children": audio_items
            })
        
        # Add decorative separator if not the last audio track
        if idx < len(audio_tracks) - 1:
            content.append({
                "tag": "p",
                "children": ["┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈"]
            })
    
    # Add decorative separator after last audio track if there are subtitles
    if subtitle_tracks:
        content.append({
            "tag": "p",
            "children": ["⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯⋯"]
        })
    
    # SUBTITLE CARD(S)
    for idx, subtitle_track in enumerate(subtitle_tracks):
        title = subtitle_track.get('Title', '')
        language_code = subtitle_track.get('Language', '')
        
        # Convert language code to full name if available
        language = language_code
        if language_code and language_code.lower() in pickFormats['audio']:  # Using audio dict for language codes
            language = pickFormats['audio'][language_code.lower()]
        
        section_title = "📝 SUBTITLE"
        if title or language:
            extras = []
            if title:
                extras.append(title)
            if language:
                extras.append(language)
            if extras:
                section_title += f" - {', '.join(extras)}"
        
        content.append({
            "tag": "h4", 
            "children": [section_title]
        })
        
        subtitle_items = []
        
        # Format
        format_name = subtitle_track.get('Format', '')
        if format_name:
            subtitle_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Format: "]},
                    format_name
                ]
            })
        
        # Language
        if language:
            subtitle_items.append({
                "tag": "li",
                "children": [
                    {"tag": "b", "children": ["Language: "]},
                    language
                ]
            })
        
        # Add subtitle items to content
        if subtitle_items:
            content.append({
                "tag": "ul",
                "children": subtitle_items
            })
        
        # Add decorative separator if not the last subtitle
        if idx < len(subtitle_tracks) - 1:
            content.append({
                "tag": "p",
                "children": ["┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈"]
            })
    
    # Add final decorative separator
    content.append({
        "tag": "p",
        "children": ["━━━━━━━━━━━━━━━━━━━━━━"]
    })
    
    return content

async def upload_to_telegraph(title, content):
    """Upload formatted media info to Telegraph."""
    try:
        response = telegraph.create_page(
            title=f"Ignite DL Info",
            content=content,
            author_name="ⓘⓖⓝⓘⓣⓔ",
            author_url="https://t.me/Ignitedlbot"
        )
        
        return f"https://telegra.ph/{response['path']}"
    except Exception as e:
        logger.error(f"Error uploading to Telegraph: {e}")
        return None

async def generate_mediainfo_link(file_path, title):
    """Generate a Telegraph link with media information."""
    try:
        # Extract media info in JSON format
        media_info = await get_media_info(file_path)
        if not media_info:
            logger.error("Failed to get media info")
            return None
            
        # Format media info for Telegraph
        content = format_media_info_telegraph(media_info, title)
        
        # Upload to Telegraph
        telegraph_url = await upload_to_telegraph(title, content)
        
        return telegraph_url
    except Exception as e:
        logger.error(f"Error generating mediainfo link: {e}")
        return None 
