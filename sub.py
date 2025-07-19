import re
from pathlib import Path
import asyncio
import aiofiles
from bs4 import BeautifulSoup

async def convert_time(time_str):
    """Convert TTML/VTT time format to SRT time format."""
    if time_str is None:
        return "00:00:00,000"
    return time_str.replace('.', ',')

async def vtt_to_srt(input_file, output_file=None):
    """Convert VTT subtitle file to SRT format."""
    try:
        async with aiofiles.open(input_file, 'r', encoding='utf-8') as f:
            vtt_content = await f.read()
            
        # Remove WEBVTT header
        vtt_content = re.sub(r'^WEBVTT\n', '', vtt_content)
        
        # Split into subtitle blocks
        blocks = re.split(r'\n\n+', vtt_content.strip())
        srt_entries = []
        
        for i, block in enumerate(blocks, 1):
            lines = block.strip().split('\n')
            if len(lines) >= 2:
                # Skip numeric identifiers in VTT
                if lines[0].strip().isdigit():
                    lines = lines[1:]
                    
                # Get timestamp line
                timestamp_line = lines[0]
                if '-->' in timestamp_line:
                    times = timestamp_line.split('-->')
                    begin = times[0].strip()
                    end = times[1].strip()
                    
                    begin_srt = await convert_time(begin)
                    end_srt = await convert_time(end)
                    
                    # Get text content
                    text = '\n'.join(lines[1:])
                    
                    if text:
                        srt_entry = f"{i}\n{begin_srt} --> {end_srt}\n{text}\n\n"
                        srt_entries.append(srt_entry)
                        
        if output_file is None:
            output_file = Path(input_file).with_suffix('.srt')
            
        async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
            for entry in srt_entries:
                await f.write(entry)
                
        return True
        
    except Exception:
        return False

async def ttml_to_srt(input_file, output_file=None):
    """Convert TTML subtitle file to SRT format."""
    try:
        async with aiofiles.open(input_file, 'r', encoding='utf-8') as f:
            ttml_content = await f.read()
        
        soup = BeautifulSoup(ttml_content, 'xml')
        srt_entries = []
        
        for i, p in enumerate(soup.find_all('p'), 1):
            try:
                begin = p.get('begin')
                end = p.get('end')
                
                if begin is None or end is None:
                    continue
                
                begin_srt = await convert_time(begin)
                end_srt = await convert_time(end)
                
                text = p.get_text(strip=True, separator='\n')
                
                if not text:
                    continue
                
                srt_entry = f"{i}\n{begin_srt} --> {end_srt}\n{text}\n\n"
                srt_entries.append(srt_entry)
                
            except Exception:
                continue
        
        if output_file is None:
            output_file = Path(input_file).with_suffix('.srt')
        
        async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
            for entry in srt_entries:
                await f.write(entry)
        
        return True
        
    except Exception:
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Determine file type and call appropriate converter
    if input_file.lower().endswith('.vtt'):
        asyncio.run(vtt_to_srt(input_file, output_file))
    else:
        asyncio.run(ttml_to_srt(input_file, output_file))