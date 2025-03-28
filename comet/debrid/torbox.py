import aiohttp
import asyncio

from RTN import parse

from comet.utils.general import is_video
from comet.utils.logger import logger


class TorBox:
    def __init__(self, session: aiohttp.ClientSession, debrid_api_key: str):
        session.headers["Authorization"] = f"Bearer {debrid_api_key}"
        self.session = session
        self.proxy = None

        self.api_url = "https://api.torbox.app/v1/api"
        self.debrid_api_key = debrid_api_key

    async def check_premium(self):
        try:
            check_premium = await self.session.get(
                f"{self.api_url}/user/me?settings=false"
            )
            check_premium = await check_premium.text()
            if '"success":true' in check_premium:
                return True
        except Exception as e:
            logger.warning(f"Exception while checking premium status on TorBox: {e}")

        return False

    async def get_instant(self, chunk: list):
        try:
            response = await self.session.get(
                f"{self.api_url}/torrents/checkcached?hash={','.join(chunk)}&format=list&list_files=true"
            )
            return await response.json()
        except Exception as e:
            logger.warning(
                f"Exception while checking hash instant availability on TorBox: {e}"
            )

    async def get_files(
        self, torrent_hashes: list, type: str, season: str, episode: str, kitsu: bool, torrents_by_hash: list = None
    ):
        chunk_size = 50
        chunks = [
            torrent_hashes[i : i + chunk_size]
            for i in range(0, len(torrent_hashes), chunk_size)
        ]

        semaphore = asyncio.Semaphore(30)  # Limit concurrent requests

        async def fetch(chunk):
            async with semaphore:
                return await self.get_instant(chunk)

        tasks = [fetch(chunk) for chunk in chunks]
        responses = await asyncio.gather(*tasks)

        availability = [response for response in responses if response is not None]

        files = {}

        if type == "series":
            for result in availability:
                if not result["success"] or not result["data"]:
                    continue

                for torrent in result["data"]:
                    torrent_files = torrent["files"]
                    for file in torrent_files:
                        filename = file["name"].split("/")[-1]

                        if not is_video(filename):
                            continue

                        if "sample" in filename.lower():
                            continue

                        filename_parsed = parse(filename)
                        if episode not in filename_parsed.episodes:
                            if 'filen' not in torrents_by_hash[torrent["hash"]]:     
                                continue
                            elif torrents_by_hash[torrent["hash"]]['filen'] != filename:
                                continue
                            filename = torrents_by_hash[torrent["hash"]]['filen']

                        if kitsu:
                            if filename_parsed.seasons:
                                continue
                        else:
                            if season not in filename_parsed.seasons:
                                if 'filen' not in torrents_by_hash[torrent["hash"]]:     
                                    continue
                                elif torrents_by_hash[torrent["hash"]]['filen'] != filename:
                                    continue
                                filename = torrents_by_hash[torrent["hash"]]['filen']

                        files[torrent["hash"]] = {
                            "index": torrent_files.index(file),
                            "title": filename,
                            "size": file["size"],
                        }

                        break
        else:
            for result in availability:
                if not result["success"] or not result["data"]:
                    continue

                for torrent in result["data"]:
                    torrent_files = torrent["files"]
                    for file in torrent_files:
                        filename = file["name"].split("/")[1]

                        if not is_video(filename):
                            continue

                        if "sample" in filename.lower():
                            continue

                        files[torrent["hash"]] = {
                            "index": torrent_files.index(file),
                            "title": filename,
                            "size": file["size"],
                        }

                        break

        return files

    async def create_torrent(self, hash: str):
        try:
            get_torrents = await self.session.get(
                f"{self.api_url}/torrents/mylist?bypass_cache=true"
            )
            get_torrents = await get_torrents.json()
            if any(torrent["hash"] == hash for torrent in get_torrents["data"]):
                return "created"

            create_torrent = await self.session.post(
                f"{self.api_url}/torrents/createtorrent",
                data={"magnet": f"magnet:?xt=urn:btih:{hash}"},
            )
            create_torrent = await create_torrent.json()
            return "created" if create_torrent["success"] else "failed"
        except Exception as e:
            logger.warning(
                f"Exception while creating torrent on TorBox for {hash}: {e}"
            )

    async def generate_download_link(self, hash: str, index: str):
        try:
            get_torrents = await self.session.get(
                f"{self.api_url}/torrents/mylist?bypass_cache=true"
            )
            get_torrents = await get_torrents.json()
            torrent = next((t for t in get_torrents["data"] if t["hash"] == hash), None)

            if not torrent:
                create_torrent = await self.session.post(
                    f"{self.api_url}/torrents/createtorrent",
                    data={"magnet": f"magnet:?xt=urn:btih:{hash}"},
                )
                create_torrent = await create_torrent.json()
                torrent_id = create_torrent["data"]["torrent_id"]
            else:
                torrent_id = torrent["id"]

            get_download_link = await self.session.get(
                f"{self.api_url}/torrents/requestdl?token={self.debrid_api_key}&torrent_id={torrent_id}&file_id={index}&zip=false",
            )
            get_download_link = await get_download_link.json()

            return get_download_link["data"]
        except Exception as e:
            logger.warning(
                f"Exception while generating download link from TorBox for {hash}|{index}: {e}"
            )
