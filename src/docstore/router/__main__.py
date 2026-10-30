"""
Entry point for running the Router: python -m docstore.router

Environment variables:
  HOST             listen host (default 127.0.0.1)
  PORT             listen port (default 8000)
  SHARD_MAP_PATH   path to static shard map file (default ./data/shard_map.json)
  CLUSTER_SECRET   >=32-char cluster shared secret
  CLIENT_API_KEY   >=16-char client API key
"""

from __future__ import annotations

import os
from pathlib import Path

import uvicorn

from docstore.common.config import get_settings
from docstore.common.logging import configure_root_logger
from docstore.router.app import build_router_app
from docstore.router.shard_map import ShardMapManager, create_default_3_shard_map


def main() -> None:
    settings = get_settings()
    configure_root_logger(settings.LOG_LEVEL)

    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))
    map_path = Path(os.environ.get("SHARD_MAP_PATH", settings.SHARD_MAP_PATH))

    mgr = ShardMapManager(path=map_path)
    if not map_path.exists():
        default_map = create_default_3_shard_map()
        mgr.save(default_map)

    app = build_router_app(settings, mgr)
    uvicorn.run(app, host=host, port=port, log_config=None)


if __name__ == "__main__":
    main()
