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

languages_emojis = {
    "unknown": "❓",  # Unknown
    "multi": "🌎",  # Dubbed
    "en": "🇬🇧",  # English
    "ja": "🇯🇵",  # Japanese
    "zh": "🇨🇳",  # Chinese
    "ru": "🇷🇺",  # Russian
    "ar": "🇸🇦",  # Arabic
    "pt": "🇵🇹",  # Portuguese
    "es": "🇪🇸",  # Spanish
    "fr": "🇫🇷",  # French
    "de": "🇩🇪",  # German
    "it": "🇮🇹",  # Italian
    "ko": "🇰🇷",  # Korean
    "hi": "🇮🇳",  # Hindi
    "bn": "🇧🇩",  # Bengali
    "pa": "🇵🇰",  # Punjabi
    "mr": "🇮🇳",  # Marathi
    "gu": "🇮🇳",  # Gujarati
    "ta": "🇮🇳",  # Tamil
    "te": "🇮🇳",  # Telugu
    "kn": "🇮🇳",  # Kannada
    "ml": "🇮🇳",  # Malayalam
    "th": "🇹🇭",  # Thai
    "vi": "🇻🇳",  # Vietnamese
    "id": "🇮🇩",  # Indonesian
    "tr": "🇹🇷",  # Turkish
    "he": "🇮🇱",  # Hebrew
    "fa": "🇮🇷",  # Persian
    "uk": "🇺🇦",  # Ukrainian
    "el": "🇬🇷",  # Greek
    "lt": "🇱🇹",  # Lithuanian
    "lv": "🇱🇻",  # Latvian
    "et": "🇪🇪",  # Estonian
    "pl": "🇵🇱",  # Polish
    "cs": "🇨🇿",  # Czech
    "sk": "🇸🇰",  # Slovak
    "hu": "🇭🇺",  # Hungarian
    "ro": "🇷🇴",  # Romanian
    "bg": "🇧🇬",  # Bulgarian
    "sr": "🇷🇸",  # Serbian
    "hr": "🇭🇷",  # Croatian
    "sl": "🇸🇮",  # Slovenian
    "nl": "🇳🇱",  # Dutch
    "da": "🇩🇰",  # Danish
    "fi": "🇫🇮",  # Finnish
    "sv": "🇸🇪",  # Swedish
    "no": "🇳🇴",  # Norwegian
    "ms": "🇲🇾",  # Malay
    "la": "💃🏻",  # Latino
}


def get_language_emoji(language: str):
    language_formatted = language.lower()
    return (
        languages_emojis[language_formatted]
        if language_formatted in languages_emojis
        else language
    )


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

translation_table = str.maketrans(translation_table)
info_hash_pattern = re.compile(r"\b([a-fA-F0-9]{40})\b")
year_pattern = re.compile(r'\d{4}\W')
s_pattern = re.compile(r"s01-s?(\d{2})")
e_pattern = re.compile(r"s\d{2}e(\d{2})-e?(\d{2})")

def translate(title: str):
    return title.translate(translation_table)


def is_video(title: str):
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
    sizes = ["Bytes", "KB", "MB", "GB", "TB"]
    if bytes == 0:
        return "0 Byte"

    i = 0
    while bytes >= 1024 and i < len(sizes) - 1:
        bytes /= 1024
        i += 1

    return f"{round(bytes, 2)} {sizes[i]}"


def size_to_bytes(size_str: str):
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
    try:
        config = orjson.loads(base64.b64decode(b64config).decode())
        validated_config = ConfigModel(**config)
        return validated_config.model_dump()
    except:
        return False


def get_debrid_extension(debridService: str, debridApiKey: str = None):
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

# Initialize cache
indexer_manager_cache = {}

async def get_indexer_manager(
    session: aiohttp.ClientSession,
    indexer_manager_type: str,
    indexers: list,
    query: str
):
    cache_key = f"query:{indexer_manager_type}:{','.join(indexers)}:{query}"
    
    # Check if results are in cache
    if cache_key in indexer_manager_cache:
        return indexer_manager_cache[cache_key]
    
    results = []
    try:
        indexers = [indexer.replace("_", " ") for indexer in indexers]

        if indexer_manager_type == "jackett":

            async def fetch_jackett_results(
                session: aiohttp.ClientSession, indexer: str, query: str
            ):
                try:
                    async with session.get(
                        f"{settings.INDEXER_MANAGER_URL}/api/v2.0/indexers/all/results?apikey={settings.INDEXER_MANAGER_API_KEY}&Query={query}&Tracker[]={indexer}",
                        timeout=aiohttp.ClientTimeout(
                            total=settings.INDEXER_MANAGER_TIMEOUT
                        ),
                    ) as response:
                        response_json = await response.json()
                        return response_json.get("Results", [])
                except Exception as e:
                    logger.warning(
                        f"Exception while fetching Jackett results for indexer {indexer}: {e}"
                    )
                    return []

            tasks = [
                fetch_jackett_results(session, indexer, query) for indexer in indexers
            ]
            all_results = await asyncio.gather(*tasks)

            for result_set in all_results:
                for result in result_set:
                    result['Seeds'] = result['Seeders']
                    if 'legendado' in result["Title"].lower():
                        result["Languages"] = ['en']
                    elif 'dual' in result["Title"].lower():
                        result["Languages"] = ['en', 'pt']
                    elif 'nacional' in result["Title"].lower():
                        result["Languages"] = ['pt']
                    elif 'dublado' in result["Title"].lower():
                        result["Languages"] = ['pt']
                results.extend(result_set)

        elif indexer_manager_type == "prowlarr":
            get_indexers = await session.get(
                f"{settings.INDEXER_MANAGER_URL}/api/v1/indexer",
                headers={"X-Api-Key": settings.INDEXER_MANAGER_API_KEY},
            )
            get_indexers = await get_indexers.json()

            indexers_id = []
            for indexer in get_indexers:
                if (
                    indexer["name"].lower() in indexers
                    or indexer["definitionName"].lower() in indexers
                ):
                    indexers_id.append(indexer["id"])

            response = await session.get(
                f"{settings.INDEXER_MANAGER_URL}/api/v1/search?query={query}&indexerIds={'&indexerIds='.join(str(indexer_id) for indexer_id in indexers_id)}&type=search",
                headers={"X-Api-Key": settings.INDEXER_MANAGER_API_KEY},
            )
            response = await response.json()

            for result in response:
                result["InfoHash"] = (
                    result["infoHash"] if "infoHash" in result else None
                )
                result["Title"] = result["title"]
                result["Size"] = result["size"]
                result["Seeds"] = result["seeders"]
                result["Link"] = (
                    result["downloadUrl"] if "downloadUrl" in result else None
                )
                result["Tracker"] = result["indexer"]
                if 'legendado' in result["Title"].lower():
                    result["Languages"] = ['en']
                elif 'dual' in result["Title"].lower():
                    result["Languages"] = ['en', 'pt']
                elif 'nacional' in result["Title"].lower():
                    result["Languages"] = ['pt']
                elif 'dublado' in result["Title"].lower():
                    result["Languages"] = ['pt']
                    

                results.append(result)
        # After fetching results
        indexer_manager_cache[cache_key] = results
    except Exception as e:
        logger.warning(
            f"Exception while getting {indexer_manager_type} results for {query} with {indexers}: {e}"
        )
        pass

    return results


async def get_zilean(
    session: aiohttp.ClientSession, name: str, log_name: str, season: int, episode: int
):
    results = []
    try:
        show = f"&season={season}&episode={episode}"
        get_dmm = await session.get(
            f"{settings.ZILEAN_URL}/dmm/filtered?query={name}{show if season else ''}"
        )
        get_dmm = await get_dmm.json()

        if isinstance(get_dmm, list):
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
        logger.wTorrentioarning(
            f"Exception while getting torrents for {log_name} with Zilean: {e}"
        )
        pass

    return results


async def get_torrentio(log_name: str, type: str, full_id: str, indexers: list, config: dict):
    results = []
    try:
        try:
            get_torrentio = requests.get(
                f"https://torrentio.strem.fun/brazuca/stream/{type}/{full_id}.json"
            ).json()
        except:
            get_torrentio = requests.get(
                f"https://torrentio.strem.fun/brazuca/stream/{type}/{full_id}.json",
                proxies={
                    "http": settings.DEBRID_PROXY_URL,
                    "https": settings.DEBRID_PROXY_URL,
                },
            ).json()

        for torrent in get_torrentio["streams"]:
            title_full = torrent["title"]
            title = title_full.split("\n")[0]
            tracker = title_full.split("⚙️ ")[1].split("\n")[0].lower()
            size = None
            seeds = None
            if "👤" in title_full and "💾" in title_full:
                try:
                    seeds = int(title_full.split("👤 ")[1].split(" ")[0])
                    size_str = title_full.split("💾 ")[1].split(" ")[0]
                    size_unit = title_full.split("💾 ")[1].split(" ")[1].split("\n")[0]
                    size = size_str + " " + size_unit
                except:
                    size = 0.0
                    seeds = 0
            languages = []
            filen = torrent['behaviorHints']['filename'] if 'behaviorHints' in torrent and 'filename' in torrent['behaviorHints'] else None
            for lang in languages_emojis:
                emoji = get_language_emoji(lang)
                if emoji in title_full:
                    languages.append(lang)

            if tracker in indexers: results.append(
                {
                    "Title": title,
                    "InfoHash": torrent["infoHash"],
                    "Size": size,
                    "Tracker": f"Torrentio|{tracker}",
                    "Languages": languages,
                    "Seeds": seeds,
                    "filen": filen
                }
            )

        logger.info(f"{len(results)} torrents found for {log_name} with Torrentio")
    except Exception as e:
        logger.warning(
            f"Exception while getting torrents for {log_name} with Torrentio, your IP is most likely blacklisted (you should try proxying Comet): {e}"
        )
        pass

    return results

async def get_ddl(
    type: str, full_id: str, season: int, episode: int
):
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
    results = []
    try:
        try:
            get_mediafusion = requests.get(
                f"{settings.MEDIAFUSION_URL}/stream/{type}/{full_id}.json"
            ).json()
        except:
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
                    ],  # not the pack size but still useful for prowlarr userss
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
    results = []
    series = type == "series"

    # Use list comprehension for simple cases
    results = [
        (index, True) if 'torrentio' in tracker.lower()
        else (index, await process_torrent(title, name, year, year_end, aliases, series, season, episode))
        for index, title, tracker in torrents
    ]

    return results

filter_cache = {}
async def process_torrent(title: str, name: str, year: int, year_end: int, aliases: dict, series: bool, season: int, episode: int) -> bool:
    cache_key = f"filter:{title}:{name}:{year}:{year_end}:{aliases}:{series}:{season}"
    filter_bool = False
    try:
        if cache_key in filter_cache:
            return filter_cache[cache_key]
        if "\n" in title:
            title = title.split("\n")[1]

        ptitle = title.split(' - ')[0]
        if not ptitle or not title_match(name, ptitle, aliases=aliases):
            return filter_bool
        
        pyear_match = year_pattern.findall(title)
        if year and pyear_match:
            pyear = int(pyear_match[0])
            if year_end is not None and not (year <= pyear <= year_end):
                return filter_bool
            if year_end is None and not (year - 1 <= pyear <= year + 1):
                return filter_bool

        if series and season is not None:
            ltitle = title.lower()
            if "s01-" in ltitle:
                s_match = s_pattern.findall(ltitle)
                if s_match and not int(s_match[0]) <= season:
                    return filter_bool
            elif e_pattern.findall(ltitle):
                # For patterns like s01e03-e05
                e_match = e_pattern.findall(ltitle)
                if e_match:
                    start_ep, end_ep = map(int, e_match[0])
                    if not (start_ep <= episode <= end_ep):
                        return filter_bool
            elif not any((str.format("s{:02d}", season) in ltitle, 'complet' in ltitle, str.format("s{:02d}e{:02d}", season, episode) in ltitle)):
                return filter_bool

        filter_bool = True
        return filter_bool
    except Exception as e:
        logger.error(f"Error processing torrent {title}: {e}")
        return False
    finally:
        filter_cache[cache_key] = filter_bool

# Initialize cache
hash_cache = {}

async def get_torrent_hash(session: aiohttp.ClientSession, torrent: tuple):
    index = torrent[0]
    torrent = torrent[1]
    if torrent.get("InfoHash") is not None:
        return (index, torrent["InfoHash"].lower())
    cache_key = f"infohash:{torrent.get('Guid')}"
    
    if cache_key in hash_cache:
        return (index, hash_cache[cache_key])
        
    url = torrent.get("Link", "")
    
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
        logger.warning(f"Error fetching torrent hash for {torrent.get('Tracker', 'Unknown')}|{url}: {e}")
    except Exception as e:
        logger.warning(f"Unexpected error fetching torrent hash for {torrent.get('Tracker', 'Unknown')}|{url}: {e}")
    
    return (index, hash_cache.get(cache_key, None))

# Initialize cache
balanced_hashes_cache = {}

def get_balanced_hashes(hashes: dict, config: dict):
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
        # Use cached evaluation
        if hash_key in balanced_hashes_cache:
            if not balanced_hashes_cache[hash_key]:
                continue
        else:
            # Evaluate hash criteria
            try:
                if remove_trash and not hash_data.get("fetch", False):
                    balanced_hashes_cache[hash_key] = False
                    continue

                hash_info = hash_data.get("data", {})

                if max_size and hash_info.get("size", 0) > max_size:
                    balanced_hashes_cache[hash_key] = False
                    continue

                if (
                    not include_all_languages
                    and not any(lang in hash_info.get("languages", []) for lang in config_languages)
                    and (("multi" not in languages) if hash_info.get("dubbed", False) else True)
                    and not (len(hash_info.get("languages", [])) == 0 and "unknown" in languages)
                ):
                    balanced_hashes_cache[hash_key] = False
                    continue

                resolution = hash_info.get("resolution", "").lower()
                if not include_all_resolutions and resolution not in config_resolutions:
                    balanced_hashes_cache[hash_key] = False
                    continue

                # Passed all checks
                balanced_hashes_cache[hash_key] = True
            except KeyError as e:
                logger.error(f"Missing key: {e}")
                balanced_hashes_cache[hash_key] = False
                continue

        resolution = hash_info.get("resolution", "").lower()
        if resolution:
            hashes_by_resolution.setdefault(resolution, []).append(hash_key)

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
    title_lower = data["Title"].lower()
    
    # Use dict comprehension for default values
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

    # Use regex for resolution matching
    for quality, patterns in QUALITY_PATTERNS.items():
        if any(pattern in title_lower for pattern in patterns):
            dat["quality"] = quality
            break

    # Simplified conditional checks
    dat["hdr"] = ["HDR10"] if "hdr10" in title_lower else ["HDR"] if "hdr" in title_lower else ["SDR"]
    dat["audio"] = ["Dolby"] if "dolby" in title_lower else ["DTS"] if "dts" in title_lower else ["Unknown"]
    dat["channels"] = ["7.1"] if "7.1" in title_lower else ["5.1"] if "5.1" in title_lower else ["2.1"]

    return dat

def format_title(data: dict, config: dict):
    result_format = config["resultFormat"]
    has_all = "All" in result_format

    title = ""
    if has_all or "Title" in result_format:
        title += f"{data['title']}\n"

    if has_all or "Metadata" in result_format:
        metadata = format_metadata(data)
        if metadata != "":
            title += f"💿 {metadata}\n"

    if has_all or "Size" in result_format and data["size"] != None:
        b = "b" in str(data['size']).lower()
        p = "." in str(data['size'])
        #title += f"💾 {bytes_to_size(int(data['size']))} " if not "." in str(data['size']) and not "b" in str(data['size']) elif "b" in str(data['size']) f"💾 {data['size']}"
        if not b and not p:
            title += f"💾 {bytes_to_size(int(data['size']))}"
        else:
            title += f"💾 {data['size']}"

    if has_all or "Tracker" in result_format:
        title += f"🔎 {data['tracker'] if 'tracker' in data else '?'}"

    if has_all or "Languages" in result_format:
        languages = data["languages"]
        if data["dubbed"]:
            languages.insert(0, "multi")
        if languages:
            formatted_languages = "/".join(
                get_language_emoji(language) for language in languages
            )
            languages_str = "\n" + formatted_languages
            title += f"{languages_str}"

    if title == "":
        # Without this, Streamio shows SD as the result, which is confusing
        title = "Empty result format configuration"

    return title


def get_client_ip(request: Request):
    return (
        request.headers["cf-connecting-ip"]
        if "cf-connecting-ip" in request.headers
        else request.client.host
    )


async def get_aliases(session: aiohttp.ClientSession, media_type: str, media_id: str):
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


