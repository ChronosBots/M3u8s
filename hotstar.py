import json
import sys
import xml.etree.ElementTree as ET
import asyncio
from pywidevine.cdm import Cdm
from pywidevine.device import Device
from pywidevine.pssh import PSSH
import aiohttp  # Add this import at the top of the file
# Import proxy configuration from helpers.config
from config import PROXY_URL, PROXIES, USE_PROXY

# Global variables for configuration
BASE_URL = "https://www.hotstar.com/api/internal/bff/v2/slugs/in"
DEBUG = False

# Proxy configuration
PROXY = PROXIES


mpd_hotstar_headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate",
    "accept-language": "eng",
    "origin": "https://www.hotstar.com",
    "referer": "https://www.hotstar.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
}

HEADERS = {
    'x-hs-usertoken': "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBJZCI6IiIsImF1ZCI6InVtX2FjY2VzcyIsImV4cCI6MTc1MTY0MTQzNCwiaWF0IjoxNzUxNTU1MDM0LCJpc3MiOiJUUyIsImp0aSI6IjY0YTVjNWY0NTRkZTRkYjc4YWNhYWI4MjU1OWZkNmRkIiwic3ViIjoie1wiaElkXCI6XCIwNDNhZGI5NWZlM2I0NGJiODkxN2EzNWMwOWQ1MjZkM1wiLFwicElkXCI6XCIwODY3OTBkZTczMjc0ZWE5YmUyNzM2ODk5MTY4ZmYxN1wiLFwiZHdIaWRcIjpcIjUyMzNjZTBiZDA0NDRkMmVkYWJmYjk3NWJhMDgyNzU4YzliOWUzZjkzNzE3YTQ3MGYxNzIxODYxODQ5ZTg3NDRcIixcImR3UGlkXCI6XCIzMzhmMWZkYWQzMjMyMGMxNmMwZjY2MjU2YzViM2VlODkyMGRjZTY3N2UwZDVlMDI2MGQzZDM0MmNhNzY3MzVhXCIsXCJvbGRIaWRcIjpcIjA0M2FkYjk1ZmUzYjQ0YmI4OTE3YTM1YzA5ZDUyNmQzXCIsXCJvbGRQaWRcIjpcIjA4Njc5MGRlNzMyNzRlYTliZTI3MzY4OTkxNjhmZjE3XCIsXCJpc1BpaVVzZXJNaWdyYXRlZFwiOmZhbHNlLFwibmFtZVwiOlwiQWJoaWppdGhcIixcInBob25lXCI6XCI2MzAwOTM2Nzc2XCIsXCJpcFwiOlwiMjQwMjo4MTAwOjI4NDU6YjA0ZTpkMmQ6Mzg4OTpiNzE6Yzc0OVwiLFwiY291bnRyeUNvZGVcIjpcImluXCIsXCJjdXN0b21lclR5cGVcIjpcIm51XCIsXCJ0eXBlXCI6XCJwaG9uZVwiLFwiaXNFbWFpbFZlcmlmaWVkXCI6ZmFsc2UsXCJpc1Bob25lVmVyaWZpZWRcIjp0cnVlLFwiZGV2aWNlSWRcIjpcIjdiOWUyOS04Yzc5YzgtNDRhNGU0LTM5OTQ5XCIsXCJwcm9maWxlXCI6XCJBRFVMVFwiLFwidmVyc2lvblwiOlwidjJcIixcInN1YnNjcmlwdGlvbnNcIjp7XCJpblwiOntcIlNpbmdsZURldmljZVwiOntcInN0YXR1c1wiOlwiU1wiLFwiZXhwaXJ5XCI6XCIyMDI1LTA5LTIzVDA5OjE4OjMxLjAwMFpcIixcInNob3dBZHNcIjpcIjFcIixcImNudFwiOlwiMVwifX19LFwiZW50XCI6XCJDdFFCQ2dVS0F3b0JCUkxLQVJJSFlXNWtjbTlwWkJJRGFXOXpFZ04zWldJU0NXRnVaSEp2YVdSMGRoSUdabWx5WlhSMkVnZGhjSEJzWlhSMkVnUnRkMlZpRWdkMGFYcGxiblIyRWdWM1pXSnZjeElHYW1sdmMzUmlFZ1J5YjJ0MUVnZHFhVzh0YkhsbUVncGphSEp2YldWallYTjBFZ1IwZG05ekVnUndZM1IyRWdOcWFXOFNCSGhpYjNnU0MzQnNZWGx6ZEdGMGFXOXVFZ1pyWlhCc1pYSWFBbk5rR2dKb1pCb0RabWhrR2dJMGF5SURjMlJ5S2daemRHVnlaVzhxQ0dSdmJHSjVOUzR4S2dwa2IyeGllVUYwYlc5eldBRUtEUklMQ0FFNEFVQUJVUEFRV0FFS0lnb2FDZ2dpQm1acGNtVjBkZ29PRWdVMU5UZ3pOaElGTmpRd05Ea1NCRGhrV0FFSzFBRUtCUW9EQ2dFQUVzb0JFZ2RoYm1SeWIybGtFZ05wYjNNU0EzZGxZaElKWVc1a2NtOXBaSFIyRWdabWFYSmxkSFlTQjJGd2NHeGxkSFlTQkcxM1pXSVNCM1JwZW1WdWRIWVNCWGRsWW05ekVnWnFhVzl6ZEdJU0JISnZhM1VTQjJwcGJ5MXNlV1lTQ21Ob2NtOXRaV05oYzNRU0JIUjJiM01TQkhCamRIWVNBMnBwYnhJRWVHSnZlQklMY0d4aGVYTjBZWFJwYjI0U0JtdGxjR3hsY2hvQ2MyUWFBbWhrR2dObWFHUWFBalJySWdOelpISXFCbk4wWlhKbGJ5b0laRzlzWW5rMUxqRXFDbVJ2YkdKNVFYUnRiM05ZQVJLREFRZ0JFTmljK0s2WE14cEhDaDFLYVc4dVNVNHVVMmx1WjJ4bFJHVjJhV05sTGxGMVlYSjBaWEpzZVJJTVUybHVaMnhsUkdWMmFXTmxHZ05LYVc4Z3dQU0dzL295S05pYytLNlhNekFHT0FOQTloTW9BVEFCT2lBS0hFaHZkSE4wWVhKUWNtVnRhWFZ0TGtsT0xqTk5iMjUwYUM0ME9Ua1FBVUlIS0pqc2c3NzVNa2dCXCIsXCJpc3N1ZWRBdFwiOjE3NTE1NTUwMzQzNzcsXCJtYXR1cml0eUxldmVsXCI6XCJBXCIsXCJpbWdcIjpcIjVcIixcImRwaWRcIjpcIjA4Njc5MGRlNzMyNzRlYTliZTI3MzY4OTkxNjhmZjE3XCIsXCJzdFwiOjEsXCJkYXRhXCI6XCJDZ1FJQUJJQUNnUUlBRG9BQ2dRSUFDb0FDZ1FJQURJQUNoSUlBQ0lPZ0FFVWlBRUJrQUhDenYrRHR5NEtCQWdBUWdBPVwifSIsInZlcnNpb24iOiIxXzAifQ.C8PihagX5KiP7wnCduHvTlkflH7wJdHOc9sCcqKpDHo",
    'X-HS-Platform': 'web',
    'X-Country-Code': 'in',
    'X-HS-Accept-language': 'eng',
    'X-Request-Id': '47722e-5bcfa6-625bf2-723fb0',
    'x-hs-device-id': '7b9e29-8c79c8-44a4e4-39949',
    'x-hs-request-id': '146ef1-3d90bb-17d53b-5d10d',
    'X-HS-Client': 'platform:androidtv;app_id:in.startv.hotstar.dplus.tv;app_version:23.08.14.4;os:Android;os_version:13;schema_version:0.0.970',
    'Origin': 'https://www.hotstar.com',
    'Referer': 'https://www.hotstar.com/in/'
}

# Load Widevine device
DEVICE = Device.load("samsung_sm-g935f.wvd")
CDM = Cdm.from_device(DEVICE)

# DRY: Default parameters
CLIENT_CAPABILITIES = {
    "ads": ["non_ssai"],
    "audio_channel": ["atmos", "dolbyatmos", "dolby51", "dolby", "stereo"],
    "container": ["fmp4", "fmp4br", "ts"],
    "dvr": ["short"],
    "dynamic_range": ["sdr", "hdr10", "dv"],
    "encryption": ["widevine", "plain"],
    "ladder": ["tv"],
    "package": ["dash", "hls"],
    "resolution": ["4k"],
    "video_codec": ["dvh265", "vp9", "h265", "h264"],
    "audio_codec": ["ac4", "ec3", "aac"],
    "true_resolution": ["4k"]
}
DRM_PARAMETERS = {
    "hdcp_version": ["HDCP_V2_2"],
    "widevine_security_level": ["SW_SECURE_DECODE"],
    "playready_security_level": []
}

def build_params(extra=None):
    p = {
        "client_capabilities": json.dumps(CLIENT_CAPABILITIES),
        "drm_parameters": json.dumps(DRM_PARAMETERS),
        "request_features": "consent_supported"
    }
    if extra:
        p.update(extra)
    return p

def extract_poster_url(seo_data):
    for widget in seo_data:
        if widget.get("template") == "SEOWidget":
            facebook_tags = widget.get("widget", {}).get("data", {}).get("facebook_tags", {})
            if facebook_tags and "ogImage" in facebook_tags:
                return facebook_tags.get("ogImage")
    return None

def extract_player_data(data):
    player = data.get("success", {}).get("page", {}).get("spaces", {}).get("player", {}).get("widget_wrappers", [{}])[0].get("widget", {}).get("data", {})
    media_asset = player.get("player_config", {}).get("media_asset", {})
    content_metadata = player.get("player_config", {}).get("content_metadata", {})
    return player, media_asset, content_metadata

async def make_request(url, method="GET", **kwargs):
    """Make HTTP request with proxy support"""
    # Prepare proxy settings if enabled
    proxy = None
    if USE_PROXY:
        proxy = PROXY['http']
    
    # Extract headers and data from kwargs
    headers = kwargs.get('headers', {})
    data = kwargs.get('data', None)
    params = kwargs.get('params', None)
    
    async with aiohttp.ClientSession() as session:
        if method == "GET":
            async with session.get(url, headers=headers, proxy=proxy, params=params) as response:
                if response.status != 200:
                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status
                    )
                response_data = await response.json()
                # Save the JSON response to a file
                with open(f"response_{url.split('/')[-1]}.json", "w") as f:
                    json.dump(response_data, f, indent=4)
                return response_data
        elif method == "POST":
            async with session.post(url, headers=headers, proxy=proxy, data=data) as response:
                if response.status != 200:
                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status
                    )
                response_data = await response.json()
                # Save the JSON response to a file
                with open(f"response_post_{url.split('/')[-1]}.json", "w") as f:
                    json.dump(response_data, f, indent=4)
                return response_data

async def setup():
    """Async setup if needed in the future"""
    pass

def extract_common_content_info(data, content_id=None, extra=None):
    player, media_asset, content_metadata = extract_player_data(data)
    hero_widget = data.get("success", {}).get("page", {}).get("spaces", {}).get("hero", {}).get("widget_wrappers", [{}])[0].get("widget", {}).get("data", {}).get("content_info", {}).get("title")
    title = hero_widget or content_metadata.get("title_cutout", {}).get("alt", "")
    title = title.replace(" - ", " ")
    poster_url = extract_poster_url(data.get("success", {}).get("page", {}).get("spaces", {}).get("seo", {}).get("widget_wrappers", []))
    mpd_url = media_asset.get("primary", {}).get("content_url", "")
    license_url = media_asset.get("primary", {}).get("license_url", "")
    pssh, subtitles = (None, [])
    if mpd_url and ".m3u8" not in mpd_url.split('?')[0]:
        # extract_pssh is async, so must be awaited in the caller
        pass
    info = {
        "id": content_id,
        "title": title,
        "mpd_url": mpd_url,
        "license_url": license_url,
        "pssh": None,  # to be filled by caller if needed
        "poster_url": poster_url,
        "subtitles": [],  # to be filled by caller if needed
    }
    if extra:
        info.update(extra)
    return info, mpd_url

def extract_episode_season_info(episode_title):
    episode_info = episode_title.split(" ", 2)
    season_number = episode_info[0].replace("S", "") if len(episode_info) > 0 else ""
    episode_number = episode_info[1].replace("E", "") if len(episode_info) > 1 else ""
    return season_number, episode_number

async def get_content_url(content_id, content_type="movies", title_slug=""):
    params = build_params()
    if "shows" in content_type:
        url = f"https://www.hotstar.com/api/internal/bff/v2/slugs/in/shows/{title_slug}/{content_id}/watch"
    else:
        url = f"{BASE_URL}/{content_type}/{title_slug}/{content_id}/watch"
    try:
        response = await make_request(url, headers=HEADERS, params=params)
        info, mpd_url = extract_common_content_info(response, content_id)
        if mpd_url and ".m3u8" not in mpd_url.split('?')[0]:
            info["pssh"], info["subtitles"] = await extract_pssh(mpd_url)
        return info
    except aiohttp.ClientError as e:
        print(f"Error: {str(e)}")
        return None

async def get_series_content(show_id, episode_id, title_slug, episode_title):
    params = build_params()
    try:
        url1 = f"https://www.hotstar.com/api/internal/bff/v2/slugs/in/shows/{title_slug}/{show_id}/episode/{episode_id}/watch"
        url2 = f"https://www.hotstar.com/api/internal/bff/v2/slugs/in/shows/{title_slug}/{show_id}/{episode_id}/watch"
        try:
            data = await make_request(url1, headers=HEADERS, params=params)
        except Exception:
            data = await make_request(url2, headers=HEADERS, params=params)
        player, _, content_metadata = extract_player_data(data)
        player_control = player.get("player_control", {})
        content_name = player_control.get("data", {}).get("content_name", {})
        episode_title_str = content_name.get("subtitle", "")
        season_number, episode_number = extract_episode_season_info(episode_title_str)
        info, mpd_url = extract_common_content_info(data, episode_id, {
            "episode_title": episode_title_str,
            "episode_number": episode_number,
            "season_number": season_number
        })
        if mpd_url and ".m3u8" not in mpd_url.split('?')[0]:
            info["pssh"], info["subtitles"] = await extract_pssh(mpd_url)
        return info
    except aiohttp.ClientError as e:
        print(f"Error: {str(e)}")
        return None

async def extract_pssh(mpd_url):
    """Extract Widevine PSSH and subtitles from MPD URL"""
    # Skip processing for m3u8 files - check URL path part before query parameters
    if '.m3u8' in mpd_url.split('?')[0]:
        print(f"Skipping PSSH extraction: URL is an m3u8 file, not an MPD file")
        return None, []
        
    try:
        # Construct curl command to get MPD content directly
        curl_command = [
            'curl',
            '--compressed',
            '--silent',  # Don't show progress
            '-H', 'accept: */*',
            '-H', 'accept-encoding: gzip, deflate',
            '-H', 'accept-language: en-US,en;q=0.9',
            '-H', 'origin: https://www.hotstar.com',
            '-H', 'referer: https://www.hotstar.com/',
            '-H', f'user-agent: {mpd_hotstar_headers["user-agent"]}',
            mpd_url
        ]
        
        if USE_PROXY:
            curl_command.extend(['--proxy', PROXY['http']])
        
        # Execute curl command asynchronously using asyncio.create_subprocess_exec
        process = await asyncio.create_subprocess_exec(
            *curl_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Wait for the process to complete and get output
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            print("Failed to fetch MPD content - curl command failed")
            return None, []
            
        # Parse MPD content directly from curl output
        try:
            root = ET.fromstring(stdout.decode('utf-8'))
        except ET.ParseError as e:
            print(f"XML parsing error: {e}")
            # Try to clean the content
            clean_content = ''.join(char for char in stdout.decode('utf-8') if ord(char) < 128)
            try:
                root = ET.fromstring(clean_content)
            except ET.ParseError as e2:
                print(f"Failed to parse even after cleaning: {e2}")
                return None, []
        
        # Define XML namespaces
        namespaces = {
            'dash': 'urn:mpeg:dash:schema:mpd:2011',
            'cenc': 'urn:mpeg:cenc:2013',
            'mspr': 'urn:microsoft:playready'
        }
        
        # Extract base URL from MPD URL for subtitles
        base_url_parts = mpd_url.split('?')[0].rsplit('/', 1)[0] + '/'
        
        # Initialize PSSH and subtitles list
        pssh_value = None
        subtitles = []
        
        # First check in Period/AdaptationSet/ContentProtection for PSSH
        for adaptation_set in root.findall('.//dash:AdaptationSet', namespaces):
            if adaptation_set.get('contentType') == 'video' or adaptation_set.get('mimeType', '').startswith('video/'):
                for content_protection in adaptation_set.findall('.//dash:ContentProtection', namespaces):
                    scheme_id_uri = content_protection.get('schemeIdUri', '')
                    if 'edef8ba9-79d6-4ace-a3c8-27dcd51d21ed' in scheme_id_uri.lower():  # Widevine
                        # Try to find pssh in cenc:pssh
                        pssh = content_protection.find('.//cenc:pssh', namespaces)
                        if pssh is not None and pssh.text:
                            pssh_value = pssh.text.strip()
                            break

        # If not found in AdaptationSet, check in Period/ContentProtection
        if not pssh_value:
            for content_protection in root.findall('.//dash:ContentProtection', namespaces):
                scheme_id_uri = content_protection.get('schemeIdUri', '')
                if 'edef8ba9-79d6-4ace-a3c8-27dcd51d21ed' in scheme_id_uri.lower():
                    pssh = content_protection.find('.//cenc:pssh', namespaces)
                    if pssh is not None and pssh.text:
                        pssh_value = pssh.text.strip()
                        break
        
        # Extract subtitles from MPD
        for adaptation_set in root.findall('.//dash:AdaptationSet[@contentType="text"]', namespaces):
            lang = adaptation_set.get('lang', 'unknown')
            
            # Get the representation element
            representation = adaptation_set.find('.//dash:Representation', namespaces)
            if representation is not None:
                # Get BaseURL
                base_url_element = representation.find('.//dash:BaseURL', namespaces)
                if base_url_element is not None and base_url_element.text:
                    # Combine with base URL if needed
                    subtitle_url = base_url_parts + base_url_element.text
                    
                    subtitle_info = {
                        "language": lang,
                        "url": subtitle_url,
                        "format": "vtt",  # Default format for Hotstar subtitles
                        "languageCode": lang.lower(),
                        "subtype": "Normal"
                    }
                    subtitles.append(subtitle_info)
        
        # Print subtitle information
        if subtitles:
            print("\nExtracted Subtitles from MPD:")
            print("="*50)
            for sub in subtitles:
                print(f"Language: {sub['language']}")
                print(f"URL: {sub['url']}")
                print(f"Format: {sub['format']}")
                print(f"Language Code: {sub['languageCode']}")
                print("-"*50)
        else:
            print("\nNo subtitles found in the MPD file.")
        
        if not pssh_value and len(subtitles) == 0:
            print("No Widevine PSSH or subtitles found in MPD content")
            
        return pssh_value, subtitles
        
    except Exception as e:
        print(f"Error extracting PSSH and subtitles: {str(e)}")
        return None, []

async def get_sports_content(sport_type, match_title, content_id, content_subtype="", language=None):
    if language is None:
        return {"error": "No language specified or invalid language"}
    params = build_params({"lang": language.lower()})
    if content_subtype == "highlights":
        url = f"{BASE_URL}/sports/{sport_type}/{match_title}/{content_id}/video/highlights/watch"
    elif content_subtype == "watch":
        url = f"{BASE_URL}/sports/{sport_type}/{match_title}/{content_id}/watch"
    else:
        url = f"{BASE_URL}/sports/{sport_type}/{match_title}/{content_id}/video/replay/watch"
    try:
        data = await make_request(url, headers=HEADERS, params=params)
        info, mpd_url = extract_common_content_info(data, content_id, {
            "sport_type": sport_type,
            "match_title": match_title,
            "content_type": content_subtype if content_subtype else "replay",
            "language": language
        })
        if mpd_url and ".m3u8" not in mpd_url.split('?')[0]:
            info["pssh"], info["subtitles"] = await extract_pssh(mpd_url)
            if info["pssh"] is None:
                print(f"Failed to extract PSSH for content ID: {content_id}")
        else:
            if not mpd_url:
                print(f"No MPD URL found for content ID: {content_id}")
        return info
    except aiohttp.ClientError as e:
        return {"error": str(e)}

async def get_keys(pssh_str, license_url):
    """Extract keys using Widevine CDM"""
    try:
        # Prepare PSSH
        pssh = PSSH(pssh_str)
        
        # Open CDM session
        session_id = CDM.open()
        
        # Get license challenge
        challenge = CDM.get_license_challenge(session_id, pssh)
        
        # Send license challenge
        async with aiohttp.ClientSession() as session:
            async with session.post(license_url, data=challenge) as response:
                if response.status != 200:
                    return None
                license_data = await response.read()
        
        # Parse license
        CDM.parse_license(session_id, license_data)
        
        # Get keys
        formatted_keys = []
        for key in CDM.get_keys(session_id):
            if hasattr(key, 'kid') and hasattr(key, 'key'):
                # Remove dashes from UUID and convert to lowercase
                kid = str(key.kid).replace('-', '')
                # Convert key bytes to hex string
                key_bytes = ''.join([f'{b:02x}' for b in key.key])
                formatted_keys.append(f"{kid}:{key_bytes}")
        
        # Close session
        CDM.close(session_id)
        
        return formatted_keys
    except Exception as e:
        return None

async def get_series_episode(series_id, season_num, episode_num, series_title):
    """Get series episode details by season and episode number"""
    try:
        # Step 1: Get series details to find season IDs
        series_url = f"https://www.hotstar.com/api/internal/bff/v2/slugs/in/shows/{series_title}/{series_id}"
        print("Found Series ID")
        
        series_response = await make_request(series_url, headers=HEADERS)
        series_data = series_response
        
        # Extract the show title from hero widget
        show_title = series_data.get("success", {}).get("page", {}).get("spaces", {}).get("hero", {}).get("widget_wrappers", [{}])[0].get("widget", {}).get("data", {}).get("content_info", {}).get("title", "")
        if not show_title:
            # Fallback to other possible locations
            show_title = series_data.get("success", {}).get("page", {}).get("spaces", {}).get("hero", {}).get("widget_wrappers", [{}])[0].get("widget", {}).get("data", {}).get("hero_img", {}).get("alt", "")
        
        # Extract season data from tabs
        season_data = None
        tray_data = series_data.get("success", {}).get("page", {}).get("spaces", {}).get("tray", {}).get("widget_wrappers", [])
        
        # Find the CategoryTrayWidget which contains the episodes
        for widget in tray_data:
            if widget.get("template") == "CategoryTrayWidget":
                category_widget = widget.get("widget", {})
                if category_widget.get("widget_commons", {}).get("id") == "EpisodeNavigation":
                    tabs = category_widget.get("data", {}).get("category_picker", {}).get("data", {}).get("tabs", [])
                    
                    for tab in tabs:
                        tab_data = tab.get("tab", {}).get("data", {})
                        if tab_data.get("title") == f"Season {season_num}":
                            season_data = tab_data
                            break
                    break
        
        if not season_data:
            return {"error": f"Season {season_num} not found"}
        
        # Extract required IDs from tray_widget_url
        tray_url = season_data.get("tray_widget_url", "")
        
        if not tray_url:
            return {"error": "Could not find episode list URL"}
            
        # Parse season IDs from tray_url
        url_params = dict(param.split("=") for param in tray_url.split("?")[1].split("&"))
        content_id = url_params.get("content_id")
        season_content_id = url_params.get("season_content_id")
        season_id = url_params.get("season_id")
        
        # Calculate page number for episode
        page_number = (episode_num + 9) // 10
        if episode_num % 10 == 0:
            page_number = episode_num // 10
        
        # Get episodes list
        episodes_url = f"https://www.hotstar.com/api/internal/bff/v2/pages/978/spaces/1445/widgets/3799/widgets/168/items"
        token = {
            "pageNo": page_number,
            "pageSize": 10,
            "sortOrder": "asc"
        }
        
        params = {
            "content_id": content_id,
            "season_content_id": season_content_id,
            "season_id": season_id,
            "token": json.dumps(token),
            "wti_name": "EpisodeNavigation"
        }
        
        episodes_response = await make_request(episodes_url, headers=HEADERS, params=params)
        episodes_data = episodes_response
        
        # Find target episode
        target_episode = None
        items = episodes_data.get("success", {}).get("widget_wrapper", {}).get("widget", {}).get("data", {}).get("items", [])
        
        for item in items:
            playable_content = item.get("playable_content", {}).get("data", {})
            tags = playable_content.get("tags", [])
            episode_tag = tags[0].get("value", "") if tags else ""
            
            if episode_tag == f"S{season_num} E{episode_num}":
                target_episode = playable_content
                print("Found Episode ID")
                break
        
        if not target_episode:
            return {"error": f"Episode {episode_num} not found in season {season_num}"}
        
        # Extract episode content ID and title
        episode_content_id = target_episode.get("download_option", {}).get("selected_id")
        if not episode_content_id:
            episode_content_id = target_episode.get("id")
        episode_title = target_episode.get("title", "").lower().replace(" ", "-")
        
        # Step 3: Get playback URL
        episode_info = await get_series_content(series_id, episode_content_id, series_title, episode_title)
        if episode_info:
            # Add the show title to the response
            episode_info["title"] = show_title
        return episode_info
        
    except Exception as e:
        return {"error": str(e)}

async def get_clip_content(clip_id, title_slug=""):
    params = build_params()
    url = f"{BASE_URL}/clips/{title_slug}/{clip_id}/watch"
    try:
        data = await make_request(url, headers=HEADERS, params=params)
        info, mpd_url = extract_common_content_info(data, clip_id, {"type": "Clip"})
        if mpd_url and ".m3u8" not in mpd_url.split('?')[0]:
            info["pssh"], info["subtitles"] = await extract_pssh(mpd_url)
        return info
    except aiohttp.ClientError as e:
        print(f"Error: {str(e)}")
        return None

def get_first_available(lst, *keys):
    for key in keys:
        val = lst.get(key, None) if isinstance(lst, dict) else None
        if val:
            return val
    return None

def get_nested(data, *keys, default=None):
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, default)
        else:
            return default
    return data

def clean_episode_title(episode_title):
    clean_title = episode_title.split(' ', 2)[-1] if len(episode_title.split(' ', 2)) > 2 else episode_title
    return ' '.join(word.capitalize() for word in clean_title.replace(" / ", " ").replace("!", "").strip().split())

def get_season_episode_num(content_info):
    try:
        season_num = int(content_info.get('season_number', 1))
        episode_num = int(content_info.get('episode_number', 1))
        return f"S{season_num:02d}E{episode_num:02d}"
    except Exception:
        return ""

def get_title(content_info, fallback):
    return content_info.get('title', '') or get_nested(content_info, 'content_metadata', 'title', default='') or fallback

async def select_language(url, language, selected_language_name):
    if language is not None and selected_language_name is not None:
        return language, selected_language_name
    parts = url.split("/sports/")[1].split("/")
    sport_type = parts[0]
    content_id = parts[-4] if "video/highlights/watch" in url or "video/replay/watch" in url else parts[-2]
    initial_url = f"{BASE_URL}/sports/{sport_type}/dummy/{content_id}/watch"
    try:
        initial_response = await make_request(initial_url, headers=HEADERS)
        player_data = get_nested(initial_response, "success", "page", "spaces", "player", "widget_wrappers", 0, "widget", "data", default={})
        available_languages = get_nested(player_data, "player_config", "content_metadata", "audio_languages", default=[])
        if available_languages:
            selected_lang = available_languages[0]
            return selected_lang['iso3code'].lower(), selected_lang['name']
        else:
            default_languages = [
                {"name": "Hindi", "iso3code": "hin"},
                {"name": "English", "iso3code": "eng"},
                {"name": "Tamil", "iso3code": "tam"},
                {"name": "Telugu", "iso3code": "tel"},
                {"name": "Bengali", "iso3code": "ben"},
                {"name": "Malayalam", "iso3code": "mal"},
                {"name": "Kannada", "iso3code": "kan"},
                {"name": "Marathi", "iso3code": "mar"}
            ]
            selected_lang = default_languages[0]
            return selected_lang['iso3code'].lower(), selected_lang['name']
    except Exception:
        return None, None

async def main(url=None, language=None, selected_language_name=None):
    await setup()
    if not url:
        print("Enter Hotstar URL: ", end="")
        url = input().strip()
    if "/sports/" in url and (language is None or selected_language_name is None):
        language, selected_language_name = await select_language(url, language, selected_language_name)
        if not language:
            print("Error fetching languages.")
            return None
    url_path = url.replace("https://www.hotstar.com/", "").replace("in/", "").strip("/")
    parts = url_path.split("/")
    content_info, sports_title, movie_title, series_title, episode_title, episode_number = { }, "", "", "", "", ""
    if parts[0].isdigit():
        content_id = parts[0]
        if len(parts) > 1 and "-" in parts[1]:
            try:
                season_num, episode_num = map(int, parts[1].split("-"))
                content_info = await get_series_episode(content_id, season_num, episode_num, str(content_id))
                if content_info:
                    series_title = get_title(content_info, f"Show {content_id}")
                    if 'episode_title' in content_info:
                        episode_title = clean_episode_title(content_info['episode_title'])
                        episode_number = get_season_episode_num(content_info)
            except (ValueError, IndexError):
                print("Invalid season-episode format")
                return
        else:
            for fn, t in [
                (get_content_url, "movies"),
                (get_content_url, "shows"),
                (get_clip_content, None)
            ]:
                content_info = await fn(content_id, t, content_id) if t else await fn(content_id, content_id)
                if content_info:
                    if t == "movies":
                        movie_title = content_info.get('title', '')
                    elif t == "shows":
                        series_title = content_info.get('title', '')
                    else:
                        movie_title = content_info.get('title', '')
                    break
    elif "/sports/" in url:
        if "/api/internal/bff/v2/slugs/in/sports/" in url:
            parts = url.split("/sports/")[1].split("/")
            sport_type, content_id, match_parts = parts[0], parts[2], [parts[1]]
            content_subtype = parts[4] if len(parts) > 4 else ""
        else:
            parts = url.split("/sports/")[1].split("/")
            sport_type, content_subtype = parts[0], ""
            if "video/highlights/watch" in url:
                content_id, match_parts, content_subtype = parts[-4], parts[1:-4], "highlights"
            elif "video/replay/watch" in url:
                content_id, match_parts, content_subtype = parts[-4], parts[1:-4], "replay"
            elif "/watch" in url:
                content_id, match_parts, content_subtype = parts[-2], parts[1:-2], "watch"
            else:
                content_id, match_parts = parts[-1], parts[1:-1]
        match_title = "/".join(match_parts)
        content_info = await get_sports_content(sport_type, match_title, content_id, content_subtype, language)
        sports_title = " ".join(part.capitalize() for part in match_title.split("-"))
    elif "/shows/" in url:
        series_parts = url.split("/")
        if len(series_parts) >= 2 and "-" in series_parts[-1] and all(n.isdigit() for n in series_parts[-1].split("-")):
            series_title_slug, series_id = series_parts[-3], series_parts[-2]
            season_num, episode_num = map(int, series_parts[-1].split("-"))
            content_info = await get_series_episode(series_id, season_num, episode_num, series_title_slug)
            if content_info:
                series_title = series_title_slug.replace("-", " ").title()
                if 'episode_title' in content_info:
                    episode_title = clean_episode_title(content_info['episode_title'])
                    episode_number = get_season_episode_num(content_info)
        else:
            parts = url.split("/shows/")[1].split("/")
            title_slug, show_id = parts[0], parts[1]
            episode_id = parts[3] if len(parts) > 3 else None
            episode_title_slug = parts[2] if len(parts) > 2 else "episode"
            content_info = await get_series_content(show_id, episode_id, title_slug, episode_title_slug)
            if content_info:
                series_title = title_slug.replace("-", " ").title()
                if 'episode_title' in content_info:
                    episode_title = clean_episode_title(content_info['episode_title'])
                    episode_number = get_season_episode_num(content_info)
    elif "/clips/" in url:
        parts = url.split("/clips/")[1].split("/")
        title_slug, content_id = parts[0], parts[1]
        content_info = await get_clip_content(content_id, title_slug)
        if content_info:
            movie_title = content_info.get('title', '')
    elif "/movies/" in url:
        parts = url.split("/movies/")[1].split("/")
        title_slug, content_id = parts[0], parts[1]
        content_info = await get_content_url(content_id, "movies", title_slug)
        if content_info:
            movie_title = content_info.get('title', '')
    if content_info and isinstance(content_info, dict) and not content_info.get('error'):
        keys = []
        if content_info.get('pssh') and content_info.get('license_url'):
            keys = await get_keys(content_info['pssh'], content_info['license_url'])
        keys_str = ",".join(keys) if keys else ""
        hls_url = ""
        if content_info.get('mpd_url', '').endswith('.m3u8'):
            hls_url = content_info.get('mpd_url', '')
            content_info['mpd_url'] = ""
        info = {
            "content_url": url,
            "platform": "JioHotstar",
            "title": sports_title or movie_title or series_title or content_info.get("title", "").strip(),
            "content_type": "SPORTS" if "/sports/" in url else ("EPISODE" if "/shows/" in url or content_info.get("episode_title") else "MOVIE"),
            "episode_title": episode_title,
            "episode_number": episode_number,
            "content_id": content_info.get("id"),
            "thumbnail": content_info.get("poster_url"),
            "streams": {"dash": content_info.get("mpd_url", ""), "hls": hls_url},
            "drm": {
                "needs_decryption": bool(content_info.get("pssh")),
                "license_url": content_info.get("license_url", ""),
                "keys": keys_str
            },
            "selected_language": selected_language_name,
            "language_code": language,
            "subtitles": content_info.get("subtitles", [])
        }
        print(json.dumps(info, indent=4))
        return info
    elif content_info and isinstance(content_info, dict) and content_info.get('error'):
        print(f"Error: {content_info['error']}")
        return None
    else:
        print("Failed to retrieve content information")
        return None

if __name__ == "__main__":
    asyncio.run(main())
