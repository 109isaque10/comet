import asyncio
import time
import aiohttp
import httpx
import uuid
import orjson

from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import (
    RedirectResponse,
    StreamingResponse,
    FileResponse,
    Response,
)
from starlette.background import BackgroundTask
from RTN import Torrent, sort_torrents

from comet.debrid.manager import getDebrid
from comet.utils.general import (
    config_check,
    get_debrid_extension,
    get_indexer_manager,
    get_ddl,
    get_zilean,
    get_torrentio,
    get_mediafusion,
    filter,
    get_torrent_hash,
    translate,
    get_balanced_hashes,
    format_title,
    get_client_ip,
    get_aliases,
    format_data,
)
from comet.utils.logger import logger
from comet.utils.models import database, rtn, settings, trackers

streams = APIRouter()

languagesEmoji = {
    "unknown": "❓",
    "multi": "🌎",
    "english": "🇬🇧",
    "portuguese": "🇵🇹",
    "spanish": "🇪🇸",
    "french": "🇫🇷",
    "german": "🇩🇪",
    "italian": "🇮🇹",
    "dutch": "🇳🇱",
    "russian": "🇷🇺",
    "japanese": "🇯🇵",
    "chinese": "🇨🇳",
    "korean": "🇰🇷",
    "arabic": "🇸🇦",
    "turkish": "🇹🇷",
}

# Initialize a single aiohttp ClientSession to be reused
session = None

@streams.on_event("startup")
async def startup_event():
    global session
    session = aiohttp.ClientSession(raise_for_status=True)

@streams.on_event("shutdown")
async def shutdown_event():
    await session.close()

@streams.get("/stream/{type}/{id}.json")
async def stream_noconfig(request: Request, type: str, id: str):
    return {
        "streams": [
            {
                "name": "[⚠️] Comet",
                "description": f"{request.url.scheme}://{request.url.netloc}/configure",
                "url": "https://comet.fast",
            }
        ]
    }

@streams.get("/{b64config}/createTorrent/{hash}")
async def create_torrent(request: Request, b64config: str, hash: str):
    config = config_check(b64config)
    if not config:
        return FileResponse("comet/assets/invalidconfig.mp4")

    if (
        settings.PROXY_DEBRID_STREAM
        and settings.PROXY_DEBRID_STREAM_PASSWORD == config["debridStreamProxyPassword"]
        and config["debridApiKey"] == ""
    ):
        config["debridService"] = settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_SERVICE
        config["debridApiKey"] = settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_APIKEY

    debrid = getDebrid(
        session,
        config,
        get_client_ip(request)
        if (
            not settings.PROXY_DEBRID_STREAM
            or settings.PROXY_DEBRID_STREAM_PASSWORD
            != config["debridStreamProxyPassword"]
        )
        else "",
    )

    check_premium = await debrid.check_premium()
    if not check_premium:
        additional_info = ""
        if config["debridService"] == "alldebrid":
            additional_info = "\nCheck your email!"

        return {
            "streams": [
                {
                    "name": "[⚠️] Comet",
                    "description": f"Invalid {config['debridService']} account.{additional_info}",
                    "url": "https://comet.fast",
                }
            ]
        }

    torrent = await debrid.create_torrent(hash)
    if torrent == "failed":
        return FileResponse("comet/assets/download_failed_v2.mp4")
    elif torrent == "created":
        return FileResponse("comet/assets/downloading_v2.mp4")

@streams.get("/{b64config}/stream/{type}/{id}.json")
async def stream(
    request: Request,
    b64config: str,
    type: str,
    id: str,
    background_tasks: BackgroundTasks,
):
    config = config_check(b64config)
    if not config:
        return {
            "streams": [
                {
                    "name": "[⚠️] Comet",
                    "description": "Invalid Comet config.",
                    "url": "https://comet.fast",
                }
            ]
        }

    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(
        connector=connector, raise_for_status=True
    ) as session:
        full_id = id
        season = None
        episode = None
        if type == "series":
            info = id.split(":")
            id = info[0]
            season = int(info[1])
            episode = int(info[2])

        year = None
        year_end = None
        try:
            kitsu = False
            if id == "kitsu":
                kitsu = True
                get_metadata = await session.get(
                    f"https://kitsu.io/api/edge/anime/{season}"
                )
                metadata = await get_metadata.json()
                name = metadata["data"]["attributes"]["canonicalTitle"]
                season = 1
            else:
                get_metadata = await session.get(
                    f"https://v3.sg.media-imdb.com/suggestion/a/{id}.json"
                )
                metadata = await get_metadata.json()
                element = metadata["d"][
                    0
                    if metadata["d"][0]["id"]
                    not in ["/imdbpicks/summer-watch-guide", "/emmys"]
                    else 1
                ]

                for element in metadata["d"]:
                    if "/" not in element["id"]:
                        break

                name = element["l"]
                year = element.get("y")

                if "yr" in element:
                    year_end = int(element["yr"].split("-")[1])
        except Exception as e:
            logger.warning(f"Exception while getting metadata for {id}: {e}")

            return {
                "streams": [
                    {
                        "name": "[⚠️] Comet",
                        "description": f"Can't get metadata for {id}",
                        "url": "https://comet.fast",
                    }
                ]
            }

        name = translate(name)
        log_name = name
        if type == "series":
            log_name = f"{name} S{season:02d}E{episode:02d}"

        if (
            settings.PROXY_DEBRID_STREAM
            and settings.PROXY_DEBRID_STREAM_PASSWORD
            == config["debridStreamProxyPassword"]
            and config["debridApiKey"] == ""
        ):
            config["debridService"] = (
                settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_SERVICE
            )
            config["debridApiKey"] = settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_APIKEY

        results = []
        if (
            config["debridStreamProxyPassword"] != ""
            and settings.PROXY_DEBRID_STREAM
            and settings.PROXY_DEBRID_STREAM_PASSWORD
            != config["debridStreamProxyPassword"]
        ):
            results.append(
                {
                    "name": "[⚠️] Comet",
                    "description": "Debrid Stream Proxy Password incorrect.\nStreams will not be proxied.",
                    "url": "https://comet.fast",
                }
            )

        debrid = getDebrid(session, config, get_client_ip(request))

        check_premium = await debrid.check_premium()
        if not check_premium:
            additional_info = ""
            if config["debridService"] == "alldebrid":
                additional_info = "\nCheck your email!"

            return {
                "streams": [
                    {
                        "name": "[⚠️] Comet",
                        "description": f"Invalid {config['debridService']} account.{additional_info}",
                        "url": "https://comet.fast",
                    }
                ]
            }

        indexer_manager_type = settings.INDEXER_MANAGER_TYPE

        search_indexer = len(config["indexers"]) != 0
        torrents = []
        tasks = []
        if indexer_manager_type and search_indexer:
            logger.info(
                f"Start of {indexer_manager_type} search for {log_name} with indexers {config['indexers']}"
            )

            search_terms = [name]
            if type == "series":
                if not kitsu:
                    search_terms.append(f"{name} S{season:02d}E{episode:02d}")
                    search_terms.append(f"{name} S{season:02d}")
                    search_terms.append(f"{name} Complet")
                    search_terms.append(f"{name} S01")
                else:
                    search_terms.append(f"{name} {episode}")
            tasks.extend(
                get_indexer_manager(
                    session, indexer_manager_type, config["indexers"], term
                )
                for term in search_terms
            )
        else:
            logger.info(
                f"No indexer {'manager ' if not indexer_manager_type else ''}{'selected by user' if indexer_manager_type else 'defined'} for {log_name}"
            )

        if settings.ZILEAN_URL:
            tasks.append(get_zilean(session, name, log_name, season, episode))

        if settings.SCRAPE_TORRENTIO:
            tasks.append(get_torrentio(log_name, type, full_id, config["indexers"], config, session))

        if settings.DDL:
            tasks.append(get_ddl(type, full_id, season, episode))

        if settings.SCRAPE_MEDIAFUSION:
            tasks.append(get_mediafusion(log_name, type, full_id))

        search_response = await asyncio.gather(*tasks)
        for results in search_response:
            if results is not None:
                for result in results:
                    torrents.append(result)

        logger.info(
            f"{len(torrents)} unique torrents found for {log_name}"
            + (
                " with "
                + ", ".join(
                    part
                    for part in [
                        indexer_manager_type,
                        "Zilean" if settings.ZILEAN_URL else None,
                        "Torrentio" if settings.SCRAPE_TORRENTIO else None,
                        "MediaFusion" if settings.SCRAPE_MEDIAFUSION else None,
                        "DDL" if settings.DDL else None,
                    ]
                    if part
                )
                if any(
                    [
                        indexer_manager_type,
                        settings.ZILEAN_URL,
                        settings.SCRAPE_TORRENTIO,
                        settings.SCRAPE_MEDIAFUSION,
                        settings.DDL,
                    ]
                )
                else ""
            )
        )

        if len(torrents) == 0:
            return {"streams": []}

        if settings.TITLE_MATCH_CHECK:
            try:
                aliases = await get_aliases(
                    session, "movies" if type == "movie" else "shows", id
                )

                indexed_torrents = [(i, torrents[i]["Title"], torrents[i]["Tracker"]) for i in range(len(torrents))]
                chunk_size = 50
                chunks = [
                    indexed_torrents[i : i + chunk_size]
                    for i in range(0, len(indexed_torrents), chunk_size)
                ]

                tasks = []
                for chunk in chunks:
                    tasks.append(
                        filter(chunk, name, year, year_end, aliases, type, season, episode)
                    )

                filtered_torrents = await asyncio.gather(*tasks)
                index_less = 0
                for result in filtered_torrents:
                    for filtered in result:
                        if not filtered[1]:
                            del torrents[filtered[0] - index_less]
                            index_less += 1
                            continue

                logger.info(
                    f"{len(torrents)} torrents passed title match check for {log_name}"
                )

                if len(torrents) == 0:
                    return {"streams": []}
            except Exception as e:
                logger.error(f"Error during title match check: {e}")
                return {"streams": []}

        tasks = []
        results = []
        f = 0
        debrid_extension = get_debrid_extension(config["debridService"])
        for i in range(len(torrents)):
            if "Domain" in torrents[i]:
                dat = torrents[i]
                dat = format_data(dat)
                if '1080p' in torrents[i]["Title"]:
                    resolution = '1080p'
                elif '720p' in torrents[i]["Title"]:
                    resolution = '720p'
                elif '480p' in torrents[i]["Title"]:
                    resolution = '480p'
                else:
                    resolution = 'Unknown'
                match torrents[i]["Debrid"]:
                    case True:
                        results.append(
                        {
                            "name": f"[{debrid_extension}⚡] Comet {resolution}",
                            "description": format_title(dat, config),
                            "url": f"{request.url.scheme}://{request.url.netloc}/{b64config}/playback/{torrents[i]['Link']}",
                            "behaviorHints": {
                                "filename": torrents[i]["Title"],
                                "bingeGroup": "comet|ddl",
                            },
                        }
                        )
                        f = 1
                    case False:
                        results.append(
                        {
                            "name": f"[{debrid_extension}⚡] Comet {resolution}",
                            "description": format_title(dat, config),
                            "url": torrents[i]["Link"],
                            "behaviorHints": {
                                "filename": torrents[i]["Title"],
                                "bingeGroup": "comet|ddl",
                            },
                        }
                        )
                        f = 1
                continue
            tasks.append(get_torrent_hash(session, (i, torrents[i])))

        torrent_hashes = await asyncio.gather(*tasks)
        index_less = 0
        for hash in torrent_hashes:
            if not hash[1]:
                del torrents[hash[0] - index_less]
                index_less += 1
                continue

            torrents[hash[0] - index_less]["InfoHash"] = hash[1]

        logger.info(f"{len(torrents)} info hashes found for {log_name}")

        if len(torrents) == 0:
            return {"streams": []}

        torrents_by_hash = {torrent["InfoHash"]: torrent for torrent in torrents if 'Domain' not in torrent}
        files = await debrid.get_files(
            list({hash[1] for hash in torrent_hashes if hash[1] is not None}),
            type,
            season,
            episode,
            kitsu,
            torrents_by_hash
        )

        ranked_files = set()
        for hash in files:
            try:
                ranked_file = rtn.rank(
                    torrents_by_hash[hash]["Title"],
                    hash,
                    remove_trash=False,  # user can choose if he wants to remove it
                )

                ranked_files.add(ranked_file)
            except:
                pass

        uncached = {}
        for hash in torrents_by_hash:
            if hash not in files:
                uncached[hash] = torrents_by_hash[hash]

        uncached_ranked_files = set()
        for hash in uncached:
            try:
                ranked_file = rtn.rank(
                    torrents_by_hash[hash]["Title"],
                    hash,
                    remove_trash=False,  # user can choose if he wants to remove it
                )

                uncached_ranked_files.add(ranked_file)
            except:
                pass

        uncached_results = []
        uncached_ranked_files = sort_torrents(uncached_ranked_files)
        uncached_ranked_files = {
            key: (value.model_dump() if isinstance(value, Torrent) else value)
            for key, value in uncached_ranked_files.items()
        }
        for hash in uncached_ranked_files:  # needed for caching
            uncached_ranked_files[hash]["data"]["title"] = torrents_by_hash[hash]["Title"]
            uncached_ranked_files[hash]["data"]["tracker"] = torrents_by_hash[hash]["Tracker"]
            uncached_ranked_files[hash]["data"]["size"] = torrents_by_hash[hash]["Size"]
            if "Languages" in torrents_by_hash[hash]:
                uncached_ranked_files[hash]["data"]["languages"] = torrents_by_hash[hash]["Languages"]
        # Apply balanced_hashes to uncached list
        balanced_uncached_hashes = get_balanced_hashes(uncached_ranked_files, config)
        sortLanguage = languagesEmoji.get(config["sortLanguage"].lower(), "🇬🇧")
        f = 1
        for resolution in balanced_uncached_hashes:
            for hash in balanced_uncached_hashes[resolution]:
                dat = uncached_ranked_files[hash]["data"]
                uncached_results.append({
                    "name": f"[{debrid_extension}⬇️] Comet {dat['quality']}",
                    "description": format_title(dat, config)+" 👤 "+str(uncached[hash]["Seeds"]),
                    "url": f"{request.url.scheme}://{request.url.netloc}/{b64config}/createTorrent/{hash}",
                    "behaviorHints": {
                        "filename": uncached[hash]["Title"],
                        "bingeGroup": "comet|"+uncached[hash]["InfoHash"],
                    },
                })
        uncached_results.sort(key=lambda x: (
            sortLanguage not in x['description'].split(" 👤 ")[0].lower(),  # Sort by language presence (Portuguese first)
            -int(x['description'].split(" 👤 ")[1])  # Sort by seeds in descending order
        ))

        sorted_ranked_files = sort_torrents(ranked_files)

        len_sorted_ranked_files = len(sorted_ranked_files)
        logger.info(
            f"{len_sorted_ranked_files} cached files found on {config['debridService']} for {log_name}"
        )

        if len_sorted_ranked_files == 0 and f == 0:
            return {"streams": []}

        sorted_ranked_files = {
            key: (value.model_dump() if isinstance(value, Torrent) else value)
            for key, value in sorted_ranked_files.items()
        }
        for hash in sorted_ranked_files:  # needed for caching
            sorted_ranked_files[hash]["data"]["title"] = files[hash]["title"]
            sorted_ranked_files[hash]["data"]["torrent_title"] = torrents_by_hash[hash]["Title"]
            sorted_ranked_files[hash]["data"]["tracker"] = torrents_by_hash[hash]["Tracker"]
            sorted_ranked_files[hash]["data"]["size"] = files[hash]["size"]
            torrent_size = torrents_by_hash[hash]["Size"]
            sorted_ranked_files[hash]["data"]["size"] = (
                files[hash]["size"]
            )
            sorted_ranked_files[hash]["data"]["torrent_size"] = (
                torrent_size if torrent_size else files[hash]["size"]
            )
            sorted_ranked_files[hash]["data"]["index"] = files[hash]["index"]
            if "Languages" in torrents_by_hash[hash]:
                sorted_ranked_files[hash]["data"]["languages"] = torrents_by_hash[hash]["Languages"]

        balanced_hashes = get_balanced_hashes(sorted_ranked_files, config)
        
        if (
            config["debridStreamProxyPassword"] != ""
            and settings.PROXY_DEBRID_STREAM
            and settings.PROXY_DEBRID_STREAM_PASSWORD
            != config["debridStreamProxyPassword"]
        ):
            results.append(
                {
                    "name": "[⚠️] Comet",
                    "description": "Debrid Stream Proxy Password incorrect.\nStreams will not be proxied.",
                    "url": "https://comet.fast",
                }
            )
        preResults = []
        for resolution in balanced_hashes:
            for hash in balanced_hashes[resolution]:
                data = sorted_ranked_files[hash]["data"]
                preResults.append(
                    {
                        "name": f"[{debrid_extension}⚡] Comet {data['resolution']}",
                        "description": format_title(data, config),
                        "torrentTitle": data["torrent_title"],
                        "torrentSize": data["torrent_size"],
                        "url": f"{request.url.scheme}://{request.url.netloc}/{b64config}/playback/{hash}/{data['index']}",
                        "behaviorHints": {
                            "filename": data["raw_title"],
                            "bingeGroup": "comet|" + hash,
                        },
                    }
                )
        preResults.sort(key=lambda x: sortLanguage not in x['description'].split("\n")[-1].lower())
        results.extend(preResults)
        results.extend(uncached_results)

        return {"streams": results}


@streams.head("/{b64config}/playback/{hash}/{index}")
async def playback(b64config: str, hash: str, index: str):
    return RedirectResponse("https://stremio.fast", status_code=302)


class CustomORJSONResponse(Response):
    media_type = "application/json"

    def render(self, content) -> bytes:
        assert orjson is not None, "orjson must be installed"
        return orjson.dumps(content, option=orjson.OPT_INDENT_2)


@streams.get("/active-connections", response_class=CustomORJSONResponse)
async def active_connections(request: Request, password: str):
    if password != settings.DASHBOARD_ADMIN_PASSWORD:
        return "Invalid Password"

    active_connections = await database.fetch_all("SELECT * FROM active_connections")

    return {
        "total_connections": len(active_connections),
        "active_connections": active_connections,
    }


@streams.get("/{b64config}/playback/{hash}/{index}")
async def playback(request: Request, b64config: str, hash: str, index: str):
    config = config_check(b64config)
    if not config:
        return FileResponse("comet/assets/invalidconfig.mp4")

    if (
        settings.PROXY_DEBRID_STREAM
        and settings.PROXY_DEBRID_STREAM_PASSWORD == config["debridStreamProxyPassword"]
        and config["debridApiKey"] == ""
    ):
        config["debridService"] = settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_SERVICE
        config["debridApiKey"] = settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_APIKEY

    ip = get_client_ip(request)
    debrid = getDebrid(
        session,
        config,
        ip
        if (
            not settings.PROXY_DEBRID_STREAM
            or settings.PROXY_DEBRID_STREAM_PASSWORD
            != config["debridStreamProxyPassword"]
        )
        else "",
    )
    download_link = await debrid.generate_download_link(hash, index)
    if not download_link:
        return FileResponse("comet/assets/uncached.mp4")

    if (
        settings.PROXY_DEBRID_STREAM
        and settings.PROXY_DEBRID_STREAM_PASSWORD
        == config["debridStreamProxyPassword"]
    ):
        if settings.PROXY_DEBRID_STREAM_MAX_CONNECTIONS != -1:
            active_ip_connections = await database.fetch_all(
                "SELECT ip, COUNT(*) as connections FROM active_connections GROUP BY ip"
            )
            if any(
                connection["ip"] == ip
                and connection["connections"]
                >= settings.PROXY_DEBRID_STREAM_MAX_CONNECTIONS
                for connection in active_ip_connections
            ):
                return FileResponse("comet/assets/proxylimit.mp4")

        proxy = None

        class Streamer:
            def __init__(self, id: str):
                self.id = id

                self.client = httpx.AsyncClient(proxy=proxy, timeout=None)
                self.response = None

            async def stream_content(self, headers: dict):
                async with self.client.stream(
                    "GET", download_link, headers=headers
                ) as self.response:
                    async for chunk in self.response.aiter_raw():
                        yield chunk

            async def close(self):
                await database.execute(
                    f"DELETE FROM active_connections WHERE id = '{self.id}'"
                )

                if self.response is not None:
                    await self.response.aclose()
                if self.client is not None:
                    await self.client.aclose()

        range_header = request.headers.get("range", "bytes=0-")

        try:
            if config["debridService"] != "torbox":
                response = await session.head(
                    download_link, headers={"Range": range_header}
                )
            else:
                response = await session.get(
                    download_link, headers={"Range": range_header}
                )
        except aiohttp.ClientResponseError as e:
            if e.status == 503 and config["debridService"] == "alldebrid":
                proxy = (
                    settings.DEBRID_PROXY_URL
                )  # proxy is needed only to proxy alldebrid streams

                response = await session.head(
                    download_link, headers={"Range": range_header}, proxy=proxy
                )
            else:
                logger.warning(f"Exception while proxying {download_link}: {e}")
                return

        if response.status == 206 or (
            response.status == 200 and config["debridService"] == "torbox"
        ):
            current_time = time.time()
            id = str(uuid.uuid4())
            await database.execute(
                f"INSERT  {'OR IGNORE ' if settings.DATABASE_TYPE == 'sqlite' else ''}INTO active_connections (id, ip, content, timestamp) VALUES (:id, :ip, :content, :timestamp){' ON CONFLICT DO NOTHING' if settings.DATABASE_TYPE == 'postgresql' else ''}",
                {
                    "id": id,
                    "ip": ip,
                    "content": str(response.url),
                    "timestamp": current_time,
                },
            )

            streamer = Streamer(id)

            return StreamingResponse(
                streamer.stream_content({"Range": range_header}),
                status_code=206,
                headers={
                    "Content-Range": response.headers["Content-Range"],
                    "Content-Length": response.headers["Content-Length"],
                    "Accept-Ranges": "bytes",
                },
                background=BackgroundTask(streamer.close),
            )

        return FileResponse("comet/assets/uncached.mp4")

    return RedirectResponse(download_link, status_code=302)

@streams.get("/{b64config}/playback/{url:path}")
async def playback(request: Request, b64config: str, url: str):
    config = config_check(b64config)
    if not config:
        return FileResponse("comet/assets/invalidconfig.mp4")

    if (
        settings.PROXY_DEBRID_STREAM
        and settings.PROXY_DEBRID_STREAM_PASSWORD == config["debridStreamProxyPassword"]
        and config["debridApiKey"] == ""
    ):
        config["debridService"] = settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_SERVICE
        config["debridApiKey"] = settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_APIKEY

    current_time = time.time()
    download_link = None

    ip = get_client_ip(request)
    if not download_link:
        debrid = getDebrid(session, config, ip)
        download_link = await debrid.generate_hoster_link(url)
        if not download_link:
            return FileResponse("comet/assets/uncached.mp4")

    if (
        settings.PROXY_DEBRID_STREAM
        and settings.PROXY_DEBRID_STREAM_PASSWORD
        == config["debridStreamProxyPassword"]
    ):
        if settings.PROXY_DEBRID_STREAM_MAX_CONNECTIONS != -1:
            active_ip_connections = await database.fetch_all(
                "SELECT ip, COUNT(*) as connections FROM active_connections GROUP BY ip"
            )
            if any(
                connection["ip"] == ip
                and connection["connections"]
                >= settings.PROXY_DEBRID_STREAM_MAX_CONNECTIONS
                for connection in active_ip_connections
            ):
                return FileResponse("comet/assets/proxylimit.mp4")

        proxy = None

        class Streamer:
            def __init__(self, id: str):
                self.id = id

                self.client = httpx.AsyncClient(proxy=proxy, timeout=None)
                self.response = None

            async def stream_content(self, headers: dict):
                async with self.client.stream(
                    "GET", download_link, headers=headers
                ) as self.response:
                    async for chunk in self.response.aiter_raw():
                        yield chunk

            async def close(self):
                await database.execute(
                    f"DELETE FROM active_connections WHERE id = '{self.id}'"
                )

                if self.response is not None:
                    await self.response.aclose()
                if self.client is not None:
                    await self.client.aclose()

        range_header = request.headers.get("range", "bytes=0-")

        try:
            if config["debridService"] != "torbox":
                response = await session.head(
                    download_link, headers={"Range": range_header}
                )
            else:
                response = await session.get(
                    download_link, headers={"Range": range_header}
                )
        except aiohttp.ClientResponseError as e:
            if e.status == 503 and config["debridService"] == "alldebrid":
                proxy = (
                    settings.DEBRID_PROXY_URL
                )  # proxy is needed only to proxy alldebrid streams

                response = await session.head(
                    download_link, headers={"Range": range_header}, proxy=proxy
                )
            else:
                logger.warning(f"Exception while proxying {download_link}: {e}")
                return

        if response.status == 206 or (
            response.status == 200 and config["debridService"] == "torbox"
        ):
            id = str(uuid.uuid4())
            await database.execute(
                f"INSERT  {'OR IGNORE ' if settings.DATABASE_TYPE == 'sqlite' else ''}INTO active_connections (id, ip, content, timestamp) VALUES (:id, :ip, :content, :timestamp){' ON CONFLICT DO NOTHING' if settings.DATABASE_TYPE == 'postgresql' else ''}",
                {
                    "id": id,
                    "ip": ip,
                    "content": str(response.url),
                    "timestamp": current_time,
                },
            )

            streamer = Streamer(id)

            return StreamingResponse(
                streamer.stream_content({"Range": range_header}),
                status_code=206,
                headers={
                    "Content-Range": response.headers["Content-Range"],
                    "Content-Length": response.headers["Content-Length"],
                    "Accept-Ranges": "bytes",
                },
                background=BackgroundTask(streamer.close),
            )

        return FileResponse("comet/assets/uncached.mp4")

    return RedirectResponse(download_link, status_code=302)

