import base64
import hashlib
import re
import aiohttp
import bencodepy
import PTT
import asyncio
import orjson

from RTN import title_match
from curl_cffi import requests
from fastapi import Request

from comet.utils.logger import logger
from comet.utils.models import settings, ConfigModel

# Mapping of language codes to corresponding emojis for display purposes.
languages_emojis = {
    "unknown": "❓",  # Unknown language
    "multi": "🌎",    # Multiple languages (Dubbed)
    "en": "🇬🇧",       # English
    "ja": "🇯🇵",       # Japanese
    "zh": "🇨🇳",       # Chinese
    "ru": "🇷🇺",       # Russian
    "ar": "🇸🇦",       # Arabic
    "pt": "🇵🇹",       # Portuguese
    "es": "🇪🇸",       # Spanish
    "fr": "🇫🇷",       # French
    "de": "🇩🇪",       # German
    "it": "🇮🇹",       # Italian
    "ko": "🇰🇷",       # Korean
    "hi": "🇮🇳",       # Hindi
    "bn": "🇧🇩",       # Bengali
    "pa": "🇵🇰",       # Punjabi
    "mr": "🇮🇳",       # Marathi
    "gu": "🇮🇳",       # Gujarati
    "ta": "🇮🇳",       # Tamil
    "te": "🇮🇳",       # Telugu
    "kn": "🇮🇳",       # Kannada
    "ml": "🇮🇳",       # Malayalam
    "th": "🇹🇭",       # Thai
    "vi": "🇻🇳",       # Vietnamese
    "id": "🇮🇩",       # Indonesian
    "tr": "🇹🇷",       # Turkish
    "he": "🇮🇱",       # Hebrew
    "fa": "🇮🇷",       # Persian
    "uk": "🇺🇦",       # Ukrainian
    "el": "🇬🇷",       # Greek
    "lt": "🇱🇹",       # Lithuanian
    "lv": "🇱🇻",       # Latvian
    "et": "🇪🇪",       # Estonian
    "pl": "🇵🇱",       # Polish
    "cs": "🇨🇿",       # Czech
    "sk": "🇸🇰",       # Slovak
    "hu": "🇭🇺",       # Hungarian
    "ro": "🇷🇴",       # Romanian
    "bg": "🇧🇬",       # Bulgarian
    "sr": "🇷🇸",       # Serbian
    "hr": "🇭🇷",       # Croatian
    "sl": "🇸🇮",       # Slovenian
    "nl": "🇳🇱",       # Dutch
    "da": "🇩🇰",       # Danish
    "fi": "🇫🇮",       # Finnish
    "sv": "🇸🇪",       # Swedish
    "no": "🇳🇴",       # Norwegian
    "ms": "🇲🇾",       # Malay
    "la": "💃🏻",       # Latino
}

def get_language_emoji(language: str):
    """
    Retrieves the emoji corresponding to a given language code.
    
    Args:
        language (str): The language code.
    
    Returns:
        str: The corresponding emoji or the original language code if not found.
    """
    language_formatted = language.lower()
    return (
        languages_emojis[language_formatted]
        if language_formatted in languages_emojis
        else language
    )

# Translation table for normalizing special characters in titles.
translation_table = {
    "ā": "a",
    "ă": "a",
    "ą": "a",
    "ć": "c",
    "č": "c",
    "ç": "c",
    "ĉ": "c",
    "ċ": "c",
    "ď": "d",
    "đ": "d",
    "è": "e",
    "é": "e",
    "ê": "e",
    "ë": "e",
    "ē": "e",
    "ĕ": "e",
    "ę": "e",
    "ě": "e",
    "ĝ": "g",
    "ğ": "g",
    "ġ": "g",
    "ģ": "g",
    "ĥ": "h",
    "î": "i",
    "ï": "i",
    "ì": "i",
    "í": "i",
    "ī": "i",
    "ĩ": "i",
    "ĭ": "i",
    "ı": "i",
    "ĵ": "j",
    "ķ": "k",
    "ĺ": "l",
    "ļ": "l",
    "ł": "l",
    "ń": "n",
    "ň": "n",
    "ñ": "n",
    "ņ": "n",
    "ŉ": "n",
    "ó": "o",
    "ô": "o",
    "õ": "o",
    "ö": "o",
    "ø": "o",
    "ō": "o",
    "ő": "o",
    "œ": "oe",
    "ŕ": "r",
    "ř": "r",
    "ŗ": "r",
    "š": "s",
    "ş": "s",
    "ś": "s",
    "ș": "s",
    "ß": "ss",
    "ť": "t",
    "ţ": "t",
    "ū": "u",
    "ŭ": "u",
    "ũ": "u",
    "û": "u",
    "ü": "u",
    "ù": "u",
    "ú": "u",
    "ų": "u",
    "ű": "u",
    "ŵ": "w",
    "ý": "y",
    "ÿ": "y",
    "ŷ": "y",
    "ž": "z",
    "ż": "z",
    "ź": "z",
    "æ": "ae",
    "ǎ": "a",
    "ǧ": "g",
    "ə": "e",
    "ƒ": "f",
    "ǐ": "i",
    "ǒ": "o",
    "ǔ": "u",
    "ǚ": "u",
    "ǜ": "u",
    "ǹ": "n",
    "ǻ": "a",
    "ǽ": "ae",
    "ǿ": "o",
}

# Compile the translation table once at module load for efficiency.
translation_table = str.maketrans(translation_table)

# Pre-compiled regex patterns for performance.
info_hash_pattern = re.compile(r"\b([a-fA-F0-9]{40})\b")
year_pattern = re.compile(r'\d{4}\W')
s_pattern = re.compile(r"s01-s?(\d{2})")
e_pattern = re.compile(r"s\d{2}e(\d{2})-e?(\d{2})")

def translate(title: str):
    """
    Translates the given title by replacing special characters based on the translation table.
    
    Args:
        title (str): The original title.
    
    Returns:
        str: The translated title.
    """
    return title.translate(translation_table)

def is_video(title: str):
    """
    Determines if the given title corresponds to a video file based on its extension.
    
    Args:
        title (str): The title or filename.
    
    Returns:
        bool: True if it's a video file, False otherwise.
    """
    video_extensions = (
        ".3g2",
        ".3gp",
        ".amv",
        ".asf",
        ".avi",
        ".drc",
        ".f4a",
        ".f4b",
        ".f4p",
        ".f4v",
        ".flv",
        ".gif",
        ".gifv",
        ".m2v",
        ".m4p",
        ".m4v",
        ".mkv",
        ".mov",
        ".mp2",
        ".mp4",
        ".mpg",
        ".mpeg",
        ".mpv",
        ".mng",
        ".mpe",
        ".mxf",
        ".nsv",
        ".ogg",
        ".ogv",
        ".qt",
        ".rm",
        ".rmvb",
        ".roq",
        ".svi",
        ".webm",
        ".wmv",
        ".yuv",
    )
    return title.endswith(video_extensions)

def bytes_to_size(bytes: int):
    """
    Converts bytes into a human-readable format.
    
    Args:
        bytes (int): The size in bytes.
    
    Returns:
        str: The size formatted as Bytes, KB, MB, GB, or TB.
    """
    sizes = ["Bytes", "KB", "MB", "GB", "TB"]
    if bytes == 0:
        return "0 Byte"

    i = 0
    while bytes >= 1024 and i < len(sizes) - 1:
        bytes /= 1024
        i += 1

    return f"{round(bytes, 2)} {sizes[i]}"

def size_to_bytes(size_str: str):
    """
    Converts a size string into bytes.
    
    Args:
        size_str (str): The size string (e.g., '2.5 GB').
    
    Returns:
        int or None: The size in bytes or None if parsing fails.
    """
    sizes = ["bytes", "kb", "mb", "gb", "tb"]
    try:
        value, unit = size_str.split()
        value = float(value)
        unit = unit.lower()

        if unit not in sizes:
            return None

        multiplier = 1024 ** sizes.index(unit)
        return int(value * multiplier)
    except:
        return None

def config_check(b64config: str):
    """
    Validates and decodes a base64-encoded configuration string.
    
    Args:
        b64config (str): The base64-encoded configuration.
    
    Returns:
        dict or bool: The decoded configuration dictionary or False if invalid.
    """
    try:
        config = orjson.loads(base64.b64decode(b64config).decode())
        validated_config = ConfigModel(**config)
        return validated_config.model_dump()
    except:
        return False

def get_debrid_extension(debridService: str, debridApiKey: str = None):
    """
    Maps a debrid service name to its corresponding extension code.
    
    Args:
        debridService (str): The name of the debrid service.
        debridApiKey (str, optional): The API key for the debrid service.
    
    Returns:
        str or None: The extension code or None if service not found.
    """
    if debridApiKey == "":
        return "TORRENT"

    debrid_extensions = {
        "realdebrid": "RD",
        "alldebrid": "AD",
        "premiumize": "PM",
        "torbox": "TB",
        "debridlink": "DL",
    }

    return debrid_extensions.get(debridService, None)

# Initialize cache for indexer manager to store previously fetched results.
indexer_manager_cache = {}

# 1. Abstract Repeated HTTP Request Logic
async def fetch_json(session: aiohttp.ClientSession, url: str, use_proxy: bool = False, headers: dict = None, timeout: aiohttp.ClientTimeout = None, result: str = None):
    """
    Helper function to fetch JSON data from a URL with optional proxy and headers.
    
    Args:
        session (aiohttp.ClientSession): The HTTP session.
        url (str): The URL to fetch.
        use_proxy (bool, optional): Whether to use proxy settings.
        headers (dict, optional): Headers to include in the request.
        timeout (aiohttp.ClientTimeout, optional): Timeout settings.
    
    Returns:
        dict or list: Parsed JSON response.
    """
    try:
        async with session.get(
            url,
            proxy=settings.DEBRID_PROXY_URL if use_proxy else None,
            headers=headers,
            timeout=timeout
        ) as response:
            response.raise_for_status()
            response = await response.json()
            if result:
                return response[result]
            return response
    except aiohttp.ClientError as e:
        logger.warning(f"HTTP error while fetching {url}: {e}")
    except asyncio.TimeoutError:
        logger.warning(f"Timeout while fetching {url}")
    except Exception as e:
        logger.warning(f"Unexpected error while fetching {url}: {e}")
    return None

# 2. Simplify Conditional Statements in `get_indexer_manager`
async def get_indexer_manager(
    session: aiohttp.ClientSession,
    indexer_manager_type: str,
    indexers: list,
    query: str
):
    """
    Fetches results from the specified indexer manager based on the type.
    
    Args:
        session (aiohttp.ClientSession): The HTTP session.
        indexer_manager_type (str): Type of indexer manager ('jackett' or 'prowlarr').
        indexers (list): List of indexers to query.
        query (str): The search query.
    
    Returns:
        list: Fetched results from the indexers.
    """
    cache_key = f"query:{indexer_manager_type}:{','.join(indexers)}:{query}"
    
    if cache_key in indexer_manager_cache:
        return indexer_manager_cache[cache_key]
    
    results = []
    try:
        indexers = [indexer.replace("_", " ") for indexer in indexers]
        
        if indexer_manager_type == "jackett":
            tasks = [
                fetch_json(session, 
                          f"{settings.INDEXER_MANAGER_URL}/api/v2.0/indexers/all/results?apikey={settings.INDEXER_MANAGER_API_KEY}&Query={query}&Tracker[]={indexer}",
                          timeout=aiohttp.ClientTimeout(total=settings.INDEXER_MANAGER_TIMEOUT),
                          result="Results")
                for indexer in indexers
            ]
            all_results = await asyncio.gather(*tasks)
            for result_set in all_results:
                if result_set:
                    for result in result_set:
                        result['Seeds'] = result.get('Seeders', 0)
                        title_lower = result["Title"].lower()
                        if 'dual' in title_lower:
                            result["Languages"] = ['en', 'pt']
                        elif 'legendado' in title_lower:
                            result["Languages"] = ['en']
                        elif 'nacional' in title_lower or 'dublado' in title_lower:
                            result["Languages"] = ['pt']
                    results.extend(result_set)
        
        elif indexer_manager_type == "prowlarr":
            indexers_data = await fetch_json(session,
                                            f"{settings.INDEXER_MANAGER_URL}/api/v1/indexer",
                                            headers={"X-Api-Key": settings.INDEXER_MANAGER_API_KEY})
            if indexers_data:
                indexers_id = [
                    indexer["id"] for indexer in indexers_data
                    if indexer["name"].lower() in indexers or indexer["definitionName"].lower() in indexers
                ]
                search_url = f"{settings.INDEXER_MANAGER_URL}/api/v1/search?query={query}&indexerIds={'&indexerIds='.join(map(str, indexers_id))}&type=search"
                search_results = await fetch_json(session,
                                                 search_url,
                                                 headers={"X-Api-Key": settings.INDEXER_MANAGER_API_KEY})
                if search_results:
                    for result in search_results:
                        result.update({
                            "InfoHash": result.get("infoHash"),
                            "Title": result.get("title"),
                            "Size": result.get("size"),
                            "Seeds": result.get("seeders", 0),
                            "Link": result.get("downloadUrl"),
                            "Tracker": result.get("indexer")
                        })
                        title_lower = result["Title"].lower()
                        if 'dual' in title_lower:
                            result["Languages"] = ['en', 'pt']
                        elif 'legendado' in title_lower:
                            result["Languages"] = ['en']
                        elif 'nacional' in title_lower or 'dublado' in title_lower:
                            result["Languages"] = ['pt']
                        results.append(result)
        indexer_manager_cache[cache_key] = results
    except Exception as e:
        logger.warning(
            f"Exception while getting {indexer_manager_type} results for {query} with {indexers}: {e}"
        )
    
    return results

async def get_zilean(
    session: aiohttp.ClientSession, name: str, log_name: str, season: int, episode: int
):
    """
    Fetches torrent results from Zilean based on the provided parameters.
    
    Args:
        session (aiohttp.ClientSession): The HTTP session.
        name (str): The name of the media.
        log_name (str): The name used for logging.
        season (int): The season number.
        episode (int): The episode number.
    
    Returns:
        list: List of torrent results.
    """
    results = []
    try:
        show = f"&season={season}&episode={episode}"
        get_dmm = await session.get(
            f"{settings.ZILEAN_URL}/dmm/filtered?query={name}{show if season else ''}"
        )
        get_dmm = await get_dmm.json()

        if isinstance(get_dmm, list):
            # Take only the first N results as configured.
            take_first = get_dmm[: settings.ZILEAN_TAKE_FIRST]
            for result in take_first:
                object = {
                    "Title": result["raw_title"],
                    "InfoHash": result["info_hash"],
                    "Size": int(result["size"]),
                    "Tracker": "DMM",
                }

                results.append(object)

        logger.info(f"{len(results)} torrents found for {log_name} with Zilean")
    except Exception as e:
        logger.warning(
            f"Exception while getting torrents for {log_name} with Zilean: {e}"
        )
        pass

    return results

# 3. Use List Comprehensions in `get_torrentio`
async def get_torrentio(log_name: str, type: str, full_id: str, indexers: list, config: dict, session: aiohttp.ClientSession):
    """
    Fetches torrent streams from Torrentio based on the provided parameters.
    
    Args:
        log_name (str): The name used for logging.
        type (str): The type of media ('series' or other).
        full_id (str): The full identifier for the media.
        indexers (list): List of indexers to filter results.
        config (dict): Configuration parameters.
    
    Returns:
        list: List of torrent results.
    """
    results = []
    try:
        try:
            get_torrentio = await fetch_json(session, f"https://torrentio.strem.fun/brazuca/stream/{type}/{full_id}.json")
        except:
            get_torrentio = await fetch_json(session, f"https://torrentio.strem.fun/brazuca/stream/{type}/{full_id}.json",
                                       use_proxy=True)
        
        for torrent in get_torrentio.get("streams", []):
            title_full = torrent["title"]
            title, tracker = title_full.split("\n")[0], title_full.split("⚙️ ")[1].split("\n")[0].lower()
            seeds = int(title_full.split("👤 ")[1].split(" ")[0]) if "👤" in title_full and "💾" in title_full else 0
            size = " ".join(title_full.split("💾 ")[1].split(" ")[:2]) if "💾" in title_full else None
            languages = [lang for lang in languages_emojis if get_language_emoji(lang) in title_full]
            filen = torrent.get('behaviorHints', {}).get('filename')
            
            if tracker in indexers:
                results.append({
                    "Title": title,
                    "InfoHash": torrent.get("infoHash"),
                    "Size": size,
                    "Tracker": f"Torrentio|{tracker}",
                    "Languages": languages,
                    "Seeds": seeds,
                    "filen": filen
                })
        
        logger.info(f"{len(results)} torrents found for {log_name} with Torrentio")
    except Exception as e:
        logger.warning(
            f"Exception while getting torrents for {log_name} with Torrentio, your IP is most likely blacklisted (you should try proxying Comet): {e}"
        )
    
    return results

async def get_ddl(
    type: str, full_id: str, season: int, episode: int
):
    """
    Fetches download links from DDL based on the provided parameters.
    
    Args:
        type (str): The type of media ('series' or other).
        full_id (str): The full identifier for the media.
        season (int): The season number.
        episode (int): The episode number.
    
    Returns:
        list: List of download results.
    """
    results = []
    try:
        if type == 'series':
            info = full_id.split(":")
            full_id = info[0]
            season_fill = str(season).zfill(2)
            episode_fill = str(episode).zfill(2)
            response = requests.get(
                    f"{settings.DDL_URL}/{full_id}_{season_fill}_{episode_fill}.json")
            debrid = False
            if response.status_code != 200:
                response = requests.get(
                f"{settings.DDL_URL}/debrid/{full_id}_{season}_{episode}.json")
                if response.status_code != 200:
                    return
                else:
                    result = response.json()
                debrid = True
            else:
                result = response.json()
            result["Title"] = result["name"]
            result["Size"] = str(result["size"])+" Gb"
            result["Link"] = result["link"]
            result["Debrid"] = debrid
            result["Tracker"] = "DDL"
            m = re.search('https?://([A-Za-z_0-9.-]+).*', result["Link"])
            result["Domain"] = m.group(1)
            results.append(result)
        else:
            response = requests.get(
                    f"{settings.DDL_URL}/{full_id}.json")
            debrid = False
            if response.status_code != 200:
                response = requests.get(
                f"{settings.DDL_URL}/debrid/{full_id}.json")
                if response.status_code != 200:
                    return
                else:
                    result = response.json()
                debrid = True
            else:
                result = response.json()
            result["Title"] = result["name"]
            result["Size"] = str(result["size"])+" Gb"
            result["Link"] = result["link"]
            result["Tracker"] = "DDL"
            result["Debrid"] = debrid
            m = re.search('https?://([A-Za-z_0-9.-]+).*', result["Link"])
            result["Domain"] = m.group(1)
            results.append(result)
    except Exception as e:
        logger.warning(
            f"Exception while getting hosters for DDL: {e}"
        )
        pass

    return results

async def get_mediafusion(log_name: str, type: str, full_id: str):
    """
    Fetches torrent streams from MediaFusion based on the provided parameters.
    
    Args:
        log_name (str): The name used for logging.
        type (str): The type of media.
        full_id (str): The full identifier for the media.
    
    Returns:
        list: List of torrent results.
    """
    results = []
    try:
        try:
            # Attempt to fetch without proxy.
            get_mediafusion = requests.get(
                f"{settings.MEDIAFUSION_URL}/stream/{type}/{full_id}.json"
            ).json()
        except:
            # Fallback to using proxy if initial request fails.
            get_mediafusion = requests.get(
                f"{settings.MEDIAFUSION_URL}/stream/{type}/{full_id}.json",
                proxies={
                    "http": settings.DEBRID_PROXY_URL,
                    "https": settings.DEBRID_PROXY_URL,
                },
            ).json()

        for torrent in get_mediafusion["streams"]:
            title_full = torrent["description"]
            title = title_full.split("\n")[0].replace("📂 ", "").replace("/", "")
            tracker = title_full.split("🔗 ")[1]

            results.append(
                {
                    "Title": title,
                    "InfoHash": torrent["infoHash"],
                    "Size": torrent["behaviorHints"][
                        "videoSize"
                    ],  # Not the pack size but still useful for Prowlarr users
                    "Tracker": f"MediaFusion|{tracker}",
                }
            )

        logger.info(f"{len(results)} torrents found for {log_name} with MediaFusion")

    except Exception as e:
        logger.warning(
            f"Exception while getting torrents for {log_name} with MediaFusion, your IP is most likely blacklisted (you should try proxying Comet): {e}"
        )
        pass

    return results

async def filter(
    torrents: list,
    name: str,
    year: int,
    year_end: int,
    aliases: dict,
    type: str,
    season: int = None,
    episode: int = None,
):
    """
    Filters the list of torrents based on various criteria.
    
    Args:
        torrents (list): List of torrents to filter.
        name (str): The name of the media.
        year (int): The release year.
        year_end (int): The end year for range filtering.
        aliases (dict): Aliases for media titles.
        type (str): The type of media ('series' or other).
        season (int, optional): Season number for series.
        episode (int, optional): Episode number for series.
    
    Returns:
        list: Filtered list of torrents with their status.
    """
    results = []
    series = type == "series"

    for index, title, tracker in torrents:
        result = await process_torrent(title, name, year, year_end, aliases, series, season)
        if 'torrentio' in tracker.lower():
            results.append((index, True))
        else:
            episode_key = f"s{season:02d}e{episode:02d}"
            if episode_key in result[1]:
                if result[0]:
                    results.append((index, True))
            else:
                e_match = e_pattern.search(result[1])
                if e_match:
                    start_ep, end_ep = map(int, e_match.groups())
                    if not (start_ep <= episode <= end_ep):
                        results.append((index, False))
                else:
                    results.append((index, result[0]))
    return results

# Cache for filtering results to avoid redundant processing.
filter_cache = {}

# 4. Enhance Exception Handling in `process_torrent`
async def process_torrent(title: str, name: str, year: int, year_end: int, aliases: dict, series: bool, season: int) -> list:
    """
    Processes a single torrent to determine if it matches the filtering criteria.
    
    Args:
        title (str): The title of the torrent.
        name (str): The name of the media.
        year (int): The release year.
        year_end (int): The end year for range filtering.
        aliases (dict): Aliases for media titles.
        series (bool): Indicates if the media is a series.
        season (int): Season number for series.
    
    Returns:
        list: A list containing a boolean indicating match and the processed title.
    """
    cache_key = f"filter:{title}:{name}:{year}:{year_end}:{series}:{season}"
    filter_bool = False
    try:
        if cache_key in filter_cache:
            return filter_cache[cache_key]
        if "\n" in title:
            title = title.split("\n")[1]
        
        ptitle = title.split(' - ')[0]
        if not ptitle or not title_match(name, ptitle, aliases=aliases):
            return [filter_bool, title]
        
        pyear_match = year_pattern.search(title)
        if pyear_match:
            pyear = int(pyear_match.group())
            if year_end and not (year <= pyear <= year_end):
                return [filter_bool, title]
            if not year_end and not (year - 1 <= pyear <= year + 1):
                return [filter_bool, title]
        
        if series and season is not None:
            ltitle = title.lower()
            if "s01-" in ltitle:
                s_match = s_pattern.search(ltitle)
                if s_match and int(s_match.group(1)) < season:
                    return [filter_bool, title]
            elif not (f"s{season:02d}" in ltitle or 'complet' in ltitle):
                return [filter_bool, title]
        
        filter_bool = True
        return [filter_bool, title]
    except Exception as e:
        logger.error(f"Error processing torrent {title}: {e}")
        return [filter_bool, title]
    finally:
        filter_cache.setdefault(cache_key, [filter_bool, title])

# Cache for storing torrent hashes to improve performance.
hash_cache = {}

# 5. Consolidate Cache Checks in `get_torrent_hash`
async def get_torrent_hash(session: aiohttp.ClientSession, torrent: tuple):
    """
    Retrieves the info hash for a given torrent, utilizing caching to enhance performance.
    
    Args:
        session (aiohttp.ClientSession): The HTTP session.
        torrent (tuple): A tuple containing the index and torrent dictionary.
    
    Returns:
        tuple: A tuple containing the index and the info hash.
    """
    index, torrent_data = torrent
    info_hash = torrent_data.get("InfoHash")
    if info_hash:
        return (index, info_hash.lower())
    
    cache_key = f"infohash:{torrent_data.get('Guid')}"
    if cache_key in hash_cache:
        return (index, hash_cache[cache_key])
    
    url = torrent_data.get("Link", "")
    try:
        timeout = aiohttp.ClientTimeout(total=settings.GET_TORRENT_TIMEOUT)
        async with session.get(url, allow_redirects=False, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}) as response:
            if response.status == 200:
                torrent_data = await response.read()
                torrent_dict = bencodepy.decode(torrent_data)
                info = bencodepy.encode(torrent_dict[b"info"])
                hash_value = hashlib.sha1(info).hexdigest().lower()
                hash_cache[cache_key] = hash_value
            else:
                location = response.headers.get("Location", "")
                if location:
                    match = info_hash_pattern.search(location)
                    if match:
                        hash_value = match.group(1).lower()
                        hash_cache[cache_key] = hash_value
    except (aiohttp.ClientError, asyncio.TimeoutError, bencodepy.BencodeDecodeError) as e:
        logger.warning(f"Error fetching torrent hash for {torrent_data.get('Tracker', 'Unknown')}|{url}: {e}")
    except Exception as e:
        logger.warning(f"Unexpected error fetching torrent hash for {torrent_data.get('Tracker', 'Unknown')}|{url}: {e}")
    
    return (index, hash_cache.get(cache_key))

# Initialize cache for balanced hashes to optimize repeated computations.
balanced_hashes_cache = {}

def get_balanced_hashes(hashes: dict, config: dict):
    """
    Balances the number of hashes based on resolution and configuration limits.
    
    Args:
        hashes (dict): Dictionary of hashes categorized by resolution.
        config (dict): Configuration parameters.
    
    Returns:
        dict: Balanced dictionary of hashes by resolution.
    """
    max_results = config["maxResults"]
    max_results_per_resolution = config["maxResultsPerResolution"]

    max_size = config["maxSize"]
    config_resolutions = [resolution.lower() for resolution in config["resolutions"]]
    include_all_resolutions = "all" in config_resolutions
    remove_trash = config["removeTrash"]

    languages = [language.lower() for language in config["languages"]]
    include_all_languages = "all" in languages
    if not include_all_languages:
        config_languages = [
            code
            for code, name in PTT.parse.LANGUAGES_TRANSLATION_TABLE.items()
            if name.lower() in languages
        ]

    hashes_by_resolution = {}
    for hash_key, hash_data in hashes.items():
        # Use cached evaluation if available.
        if hash_key in balanced_hashes_cache:
            if not balanced_hashes_cache[hash_key]:
                continue
        else:
            # Evaluate hash criteria.
            try:
                if remove_trash and not hash_data["fetch"]:
                    balanced_hashes_cache[hash_key] = False
                    continue

                hash_info = hash_data["data"]

                if max_size != 0 and hash_info["size"] > max_size:
                    balanced_hashes_cache[hash_key] = False
                    continue

                if (
                    not include_all_languages
                    and not any(lang in hash_info["languages"] for lang in config_languages)
                    and (("multi" not in languages) if hash_info["dubbed"] else True)
                    and not (len(hash_info["languages"]) == 0 and "unknown" in languages)
                ):
                    balanced_hashes_cache[hash_key] = False
                    continue

                resolution = hash_info["resolution"].lower()
                if not include_all_resolutions and resolution not in config_resolutions:
                    balanced_hashes_cache[hash_key] = False
                    continue

                # Passed all checks.
                balanced_hashes_cache[hash_key] = True
            except KeyError as e:
                logger.error(f"Missing key: {e}")
                balanced_hashes_cache[hash_key] = False
                continue

        hash_info = hash_data["data"]
        resolution = hash_info["resolution"].lower()
        if resolution:
            hashes_by_resolution.setdefault(resolution, []).append(hash_key)

    # Reverse the order of hashes if configured.
    if config["reverseResultOrder"]:
        hashes_by_resolution = {
            res: lst[::-1] for res, lst in hashes_by_resolution.items()
        }

    total_resolutions = len(hashes_by_resolution)
    if (max_results == 0 and max_results_per_resolution == 0) or total_resolutions == 0:
        return hashes_by_resolution

    hashes_per_resolution = (
        max_results // total_resolutions
        if max_results > 0
        else max_results_per_resolution
    )
    extra_hashes = max_results % total_resolutions

    balanced_hashes = {}
    for resolution, hash_list in hashes_by_resolution.items():
        selected_count = hashes_per_resolution + (1 if extra_hashes > 0 else 0)
        if max_results_per_resolution > 0:
            selected_count = min(selected_count, max_results_per_resolution)
        balanced_hashes[resolution] = hash_list[:selected_count]
        if extra_hashes > 0:
            extra_hashes -= 1

    selected_total = sum(len(hashes) for hashes in balanced_hashes.values())
    if selected_total < max_results:
        missing_hashes = max_results - selected_total
        for resolution, hash_list in hashes_by_resolution.items():
            if missing_hashes <= 0:
                break
            current_count = len(balanced_hashes[resolution])
            available_hashes = hash_list[current_count : current_count + missing_hashes]
            balanced_hashes[resolution].extend(available_hashes)
            missing_hashes -= len(available_hashes)

    return balanced_hashes

def format_metadata(data: dict):
    """
    Formats metadata information into a single string separated by '|'.
    
    Args:
        data (dict): Dictionary containing metadata fields.
    
    Returns:
        str: Formatted metadata string.
    """
    extras = []
    if data["quality"]:
        extras.append(data["quality"])
    if data["hdr"]:
        extras.extend(data["hdr"])
    if data["codec"]:
        extras.append(data["codec"])
    if data["audio"]:
        extras.extend(data["audio"])
    if data["channels"]:
        extras.extend(data["channels"])
    if data["bit_depth"]:
        extras.append(data["bit_depth"])
    if data["network"]:
        extras.append(data["network"])
    if data["group"]:
        extras.append(data["group"])

    return "|".join(extras)

QUALITY_PATTERNS = {
    "4K": ["2160p", "4k"],
    "1080p": ["1080p", "fullhd"],
    "720p": ["720p", "hd"],
    "SD": ["480p", "sd"]
}

def format_data(data: dict):
    """
    Formats torrent data by extracting and categorizing various attributes.
    
    Args:
        data (dict): Dictionary containing torrent data.
    
    Returns:
        dict: Formatted data dictionary with categorized attributes.
    """
    title_lower = data["Title"].lower()
    
    # Initialize default values using dictionary comprehension.
    dat = {
        "title": data["Title"],
        "tracker": data["Tracker"],
        "size": data["Size"],
        "languages": data.get("Languages", ["English"]),
        "quality": "Unknown",
        "hdr": ["SDR"],
        "audio": ["Unknown"],
        "channels": ["2.1"],
        "bit_depth": "",
        "network": "",
        "group": "",
        "codec": "",
        "dubbed": ""
    }

    # Determine quality based on predefined patterns.
    for quality, patterns in QUALITY_PATTERNS.items():
        if any(pattern in title_lower for pattern in patterns):
            dat["quality"] = quality
            break

    # Determine HDR type.
    dat["hdr"] = ["HDR10"] if "hdr10" in title_lower else ["HDR"] if "hdr" in title_lower else ["SDR"]
    # Determine audio type.
    dat["audio"] = ["Dolby"] if "dolby" in title_lower else ["DTS"] if "dts" in title_lower else ["Unknown"]
    # Determine audio channels.
    dat["channels"] = ["7.1"] if "7.1" in title_lower else ["5.1"] if "5.1" in title_lower else ["2.1"]

    return dat

def format_title(data: dict, config: dict):
    """
    Constructs the display title based on the configuration and data.
    
    Args:
        data (dict): Dictionary containing torrent data.
        config (dict): Configuration parameters.
    
    Returns:
        str: Formatted title string.
    """
    result_format = config["resultFormat"]
    has_all = "All" in result_format

    title = ""
    if has_all or "Title" in result_format:
        title += f"{data['title']}\n"

    if has_all or "Metadata" in result_format:
        metadata = format_metadata(data)
        if metadata != "":
            title += f"💿 {metadata}\n"

    if has_all or ("Size" in result_format and data["size"] != None):
        b = "b" in str(data['size']).lower()
        p = "." in str(data['size'])
        if not b and not p:
            title += f"💾 {bytes_to_size(int(data['size']))}"
        else:
            title += f"💾 {data['size']}"

    if has_all or "Tracker" in result_format:
        title += f"🔎 {data['tracker'] if 'tracker' in data else '?'}"

    if has_all or "Languages" in result_format:
        languages = data["languages"]
        if data["dubbed"]:
            if not "multi" in languages:
                languages.insert(0, "multi")
        if languages:
            formatted_languages = "/".join(
                get_language_emoji(language) for language in languages
            )
            languages_str = "\n" + formatted_languages
            title += f"{languages_str}"

    if title == "":
        # Default message to avoid confusion in Streamio.
        title = "Empty result format configuration"

    return title

def get_client_ip(request: Request):
    """
    Retrieves the client's IP address from the request headers.
    
    Args:
        request (Request): The incoming HTTP request.
    
    Returns:
        str: The client's IP address.
    """
    return (
        request.headers["cf-connecting-ip"]
        if "cf-connecting-ip" in request.headers
        else request.client.host
    )

async def get_aliases(session: aiohttp.ClientSession, media_type: str, media_id: str):
    """
    Fetches aliases for a given media from the Trakt.tv API.
    
    Args:
        session (aiohttp.ClientSession): The HTTP session.
        media_type (str): The type of media ('movie' or 'series').
        media_id (str): The identifier for the media.
    
    Returns:
        dict: A dictionary of aliases categorized by country.
    """
    aliases = {}
    try:
        response = await session.get(
            f"https://api.trakt.tv/{media_type}/{media_id}/aliases"
        )

        for aliase in await response.json():
            country = aliase["country"]
            if country not in aliases:
                aliases[country] = []

            aliases[country].append(aliase["title"])
    except:
        pass

    return aliases


