# Define proxies
USE_PROXY = True  # Set to True to enable proxy usage
PROXY_FIRST = True

# Proxy Configuration
PROXY_USERNAME = "paypalmafiabots"
PROXY_PASSWORD = "Aryan"
PROXY_HOST = "103.171.50.6"
PROXY_PORT = 50100




# http://nx1botz0zIn6:sVF4DJXFt8@103.167.32.218:49155
# http://paypalmafiabots:TeamUniverse@103.235.64.29:50100
# Format: protocol://username:password@host:port
PROXY_URL = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"

# http://paypalmafiabots:Gdha@103.172.84.29:50100

# Proxy dictionary for requests
PROXIES = {
    "http": PROXY_URL,
    "https": PROXY_URL
}

MP4_USER_IDS = {"1822859631"}  # User IDs that get mp4 extension instead of mkv

# Whether to keep and dump streams after muxing (True) or delete them immediately (False)
DUMP_STREAMS = False

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

def get_language_name(audio_locale):
    """Convert language code to full name"""
    # Add mapping for language codes to full names
    language_map = {'ja-JP': 'Japanese', 'en-US': 'English', 'es-419': 'Spanish', 'pt-BR': 'Portuguese', 'de-DE': 'German', 'hi-IN': 'Hindi', 'fr-FR': 'French', 'it-IT': 'Italian',
        'es-ES': 'Spanish', 'ta-IN': 'Tamil', 'te-IN': 'Telugu', 'ko-KR': 'Korean', 'ru-RU': 'Russian', 'ar-ME': 'Arabic', 'tr-TR': 'Turkish', 'vi-VN': 'Vietnamese',
        'th-TH': 'Thai', 'zh-CN': 'Chinese', 'zh-TW': 'Chinese', 'id-ID': 'Indonesian', 'ms-MY': 'Malay', 'fil-PH': 'Filipino', 'bn-IN': 'Bengali', 'gu-IN': 'Gujarati',
        'kn-IN': 'Kannada', 'ml-IN': 'Malayalam', 'mr-IN': 'Marathi', 'or-IN': 'Odia', 'pa-IN': 'Punjabi', 'as-IN': 'Assamese', 'ks-IN': 'Kashmiri', 'sa-IN': 'Sanskrit',
        'ur-IN': 'Urdu', 'mai-IN': 'Maithili', 'bho-IN': 'Bhojpuri', 'ne-IN': 'Nepali', 'sd-IN': 'Sindhi', 'sat-IN': 'Santali', 'doi-IN': 'Dogri', 'raj-IN': 'Rajasthani',
        'uk-UA': 'Ukrainian', 'pl-PL': 'Polish', 'cs-CZ': 'Czech', 'sk-SK': 'Slovak', 'hu-HU': 'Hungarian', 'ro-RO': 'Romanian', 'bg-BG': 'Bulgarian', 'hr-HR': 'Croatian',
        'sr-RS': 'Serbian', 'sl-SI': 'Slovenian', 'el-GR': 'Greek', 'he-IL': 'Hebrew', 'fa-IR': 'Persian', 'sw-KE': 'Swahili', 'am-ET': 'Amharic', 'ka-GE': 'Georgian',
        'hy-AM': 'Armenian', 'az-AZ': 'Azerbaijani', 'uz-UZ': 'Uzbek', 'tg-TJ': 'Tajik', 'tk-TM': 'Turkmen', 'ky-KG': 'Kyrgyz', 'mn-MN': 'Mongolian', 'my-MM': 'Burmese',
        'km-KH': 'Khmer', 'lo-LA': 'Lao'}
    return language_map.get(audio_locale, audio_locale)

# ISO 639-1 to ISO 639-2 mapping
def get_iso_639_2(lang_code):
    iso_map = {
        'en': 'eng', 'de': 'deu', 'es': 'spa', 'fr': 'fra', 'it': 'ita', 'tr': 'tur',
        'hi': 'hin', 'ta': 'tam', 'te': 'tel', 'kn': 'kan', 'ml': 'mal', 'bn': 'ben',
        'gu': 'guj', 'mr': 'mar', 'pa': 'pan', 'ar': 'ara', 'zh': 'zho', 'ja': 'jpn',
        'ko': 'kor', 'ru': 'rus', 'pt': 'por', 'nl': 'nld', 'pl': 'pol', 'vi': 'vie',
        'id': 'ind', 'th': 'tha', 'sv': 'swe', 'da': 'dan', 'fi': 'fin', 'no': 'nor',
        'cs': 'ces', 'el': 'ell', 'he': 'heb', 'ro': 'ron', 'hu': 'hun', 'uk': 'ukr',
        'ms': 'msa'  # Added Malay language code
    }
    return iso_map.get(lang_code, lang_code)

# OTT ACCOUNTS TOKENS

## AHA (fix)
## DEVICE_ID = "b7786cf5-89de-4414-ae58-0b5c96af07fa"

## AMAZON PRIME
COOKIE = 'session-token=y5Bf0tIswTT0SDHS0F5Tj3s2AUggQIB/mUo5dDT2WsYxrHaJBFbdPb7JQapCEhTNH7NwcqddODqyhoUQEIgJtsR9DOkgxECaat1oACCdBsN9gDPU7GhnKjTLKAvLUqYo6KQu4uTmksy9gr2AOVa1Gce9KK+QlM8RSmPzQF4+XVnHTfRFucdW/0dgDLctH9VNKFkJil2LSA/aL9a/hgRIV0Sl4jigdX5vBcmpXpAaPkLdmGx+LyXzmG0NlaOEZeNj1OGErkPCKmDpKYoH9M1itYtW8XgbJ/YAXQlixx1fOhY0eOcWYVg3ewN6S4gTRTiryITd4r1kxx8hGZ71UqcXVodz0Pvcv9bDR2qdornLzCuZL+1cFnssErH9QE/ZzPxtuiWbDzRydyGtA3c3cZU5CWQYwf8FttGEDf9xLwiBAXAyjZPxfPHEgA==;csm-hit=tb:s-MP2Y7ZEJJEH1D0NYFKAA|1747158491106&t:1747158491171&adb:adblk_yes;i18n-prefs=USD;x-main-av=pEGCjaWVSvmZd@LxopD3xzFDgC1WpBrKOUg5?3j54274O4ZrbJc2qV?gZnEYFiyX;sess-at-main-av=EOCaMCIGB6pECPNpKHgwUHOUtq6YuYdE+iuXr79pPTE=;av-profile=cGlkPWFtem4xLmFjdG9yLnBlcnNvbi5vaWQuQTJKVkNJMThWRDlNOEQmdGltZXN0YW1wPTE3NDcxNTg0Mzg3NDAmdmVyc2lvbj12MQ.g59jhKz2fPCli7EwiVP3RpEngeUlSE3wtshJrzcMxJROAAAAAQAAAABoI4WmcmF3AAAAAPgWC9WfHH8iB-olH_E9xQ;lc-main-av=en_US;at-main-av=Atza|IwEBIO329WM8NVujPaSyrP8I7oiK81_42pKokitmCmlf28sq3aUyD0CnMg9UBV_5XpBthObAzF5-H0RWS71EqpAFmzKjQUzzkY0b442OTpF0O4wWGzAuv_CS0oUyVKdvmlX_O33ym5LhvRlhVI-RAKS7m9F_FNYvkkemGAl_qHxlJYysfIyTN5vqZd6aNGMtDyVRzskse_e2FxZNx3PFaNhOR6MHJJhDVlTTzCi603oA1et1RepWIYrTMDiGDHlpZ4WTPTFgIcwl1b76iYJisSh6_AG4m0m-WJ_jTyeqBrJE-_2kZ4vJEOivYgFRO_Luj479Y5YVuh_1kLccUzc2IQ3v5FRhlhcaavD_IzhAe12pGXCfQA;session-id-time=2082787201l;session-id=258-4041037-6486900;ubid-main-av=259-6576861-8680310'

## CRUNCHYROLL
COOKIE_CONFIG = "device_id=c1b89a32-d1fd-4723-860f-75375ce9cb25; c_locale=en-US; device_id=c1b89a32-d1fd-4723-860f-75375ce9cb25; __cf_bm=_KgJ9cfKkpJBUAMyaNMJ8Ha4rQUcWGKK0U.G_lzqKTU-1752247527-1.0.1.1-ZCHYZLQqBgdwd3BVFfoDoF7donJn_4mehzzTNKoAYImRdaqM7puL75OThpIBpfU1APyocX2TNu1r_d88d.y_j9R2O.P2lnt_Qti_lcnrKQ6xuBsp2xfcPnHimP.jW8Km; ajs_anonymous_id=d69a26c7-b2c6-46ea-a404-ceef12796528; etp_rt=c2c8c20c-8adb-4384-b4c6-f235d0b964ef; cf_clearance=jQGlAZqdDgVGd01QYv8QKd92ihby3Ndcyi52Fzsp9_U-1752247596-1.2.1.1-iH5P7WTVbR1ND1WLtdCV3gW6gFK0X9vOQgJeyXJBlN3AZv5WKxe.jJqZ0H1xVgl1t_heI4z4r8oMkws4Xm7CNTECsmRf.IaLxDPhFEfIkmNWW.jybCfM.xVHTNBUDBJR1jI_OvJEdpvW6QE2leH.xLWBvwpfecGvfFvTlJnDBRAorYgoKFz6GZazkeIMYeAxo3E_IOt8zsLsJcoh.sL7PTHy7FAgRxUJ.Dah5f02s5g; ab.storage.userId.80f403d2-1c18-471d-b0ef-243d1d646436=%7B%22g%22%3A%2298aff2b2-7122-5d3c-8496-26c260c24bfe%22%2C%22c%22%3A1752247600787%2C%22l%22%3A1752247600788%7D; ab.storage.deviceId.80f403d2-1c18-471d-b0ef-243d1d646436=%7B%22g%22%3A%22d2a4d8ff-e5e2-8cc2-fad4-cd9bb92ac74c%22%2C%22c%22%3A1752247600789%2C%22l%22%3A1752247600789%7D; ab.storage.sessionId.80f403d2-1c18-471d-b0ef-243d1d646436=%7B%22g%22%3A%22dadb520e-1837-b35e-fb27-227356327b6a%22%2C%22e%22%3A1752249400797%2C%22c%22%3A1752247600788%2C%22l%22%3A1752247600797%7D; _dd_s=rum=0&expire=1752248799043"

## CHAUPAL TV
API_KEY = "AIzaSyCy9pm1PChZKOULywz9FBV1QD8MLZFc35c"
REFRESH_TOKEN = "AMf-vByDvonlbf_sQ3yPRdZkpYLEsVLaU8LTH_znUizaQRXcUxfu2qfsUMcRmPKO8qfpcMgv8s-3R3kjLrnzUG7M1Mq_JToL1nP04rB6ySCEGW1vdMuFEcxFRFrz7zyZczb2-ok4FjkWVJN2RX_SHkW3AznNEYFA_Xn-u9NYCADasuNkpebmOVs85AqYhyBaGFXPITfs3pfzbcZDGlRo7ZpGug8zvcgYEBxP5KDc71wsCQb7OTLECew"
CACHE_FILE = "token_cache.json"
BUFFER_MINUTES = 5

## DISCOVERY PLUS+
DPLUS_ACCESS_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJVU0VSSUQ6ZHBsdXNpbmRpYTphMmY0ODlkNi05ZjBjLTQ5NjQtYWYxNi04YTBhYjIzNjMyZTkiLCJqdGkiOiJ0b2tlbi1kOTUzODc0ZC1lOGMyLTQ0N2YtYjBiMS0yNjA5ZWEzY2QzYzYiLCJhbm9ueW1vdXMiOmZhbHNlLCJpYXQiOjE3NTA5NDM3Mjh9.gj3brc9rnd3HtkbPh7kUxND-hBxnrGc2ZIRNmWK56bQ'

## SONY LIV
DEVICE_ID = "e55eac3f6aa64c218808aa516f905c2b-1752249266062"
SESSION_ID = "e55eac3f6aa64c218808aa516f905c2b-1752249266062"
AUTHORIZATION_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiIyMjEwMjUxNTExMjk3NDAwNTAxIiwidG9rZW4iOiJmbWVYLUxGcXktM0NjNi1kV3VVLW10bEUtRFIxUi04aCIsImV4cGlyYXRpb25Nb21lbnQiOiIyMDI2LTA3LTExVDE1OjU1OjEzLjkxNVoiLCJpc1Byb2ZpbGVDb21wbGV0ZSI6dHJ1ZSwic2Vzc2lvbkNyZWF0aW9uVGltZSI6IjIwMjUtMDctMTFUMTU6NTU6MTMuOTE2WiIsImNoYW5uZWxQYXJ0bmVySUQiOiJNU01JTkQiLCJmaXJzdE5hbWUiOiJNYWhlc2giLCJtb2JpbGVOdW1iZXIiOiI4MzMxOTE1NTc4IiwiZGF0ZU9mQmlydGgiOjYzOTI1MzgwMDAwMCwiZ2VuZGVyIjoiTWFsZSIsInByb2ZpbGVQaWMiOiJodHRwczovL29yaWdpbi1zdGF0aWN2Mi5zb255bGl2LmNvbS9VSV9pY29ucy9Nb2JpbGVfQXZhdGFyc18wMy5wbmciLCJzb2NpYWxQcm9maWxlUGljIjoiIiwic29jaWFsTG9naW5JRCI6bnVsbCwic29jaWFsTG9naW5UeXBlIjpudWxsLCJpc0VtYWlsVmVyaWZpZWQiOnRydWUsImlzTW9iaWxlVmVyaWZpZWQiOnRydWUsImxhc3ROYW1lIjoiIiwiZW1haWwiOiJkaGFuYWxha3NobWlrYWRhbGkxOTkwQGdtYWlsLmNvbSIsImlzQ3VzdG9tZXJFbGlnaWJsZUZvckZyZWVUcmlhbCI6ZmFsc2UsImNvbnRhY3RJRCI6IjMyODY5MjA2MiIsImlhdCI6MTc1MjI0OTMxNCwiZXhwIjoxNzgzNzg1MzE0fQ.MsOw2GR5N_UsPNfgm7zuYkuMPLzLgfTePadl2SUZq3c"

## ULLU
AUTH_TOKEN =  "Bearer f0c1e4e9-1cc0-416e-9b22-d03c98bafbaa"
