"""
Entry point: python -m docstore.shard

Environment variables (all required unless defaults are noted):
  NODE_ID               unique node identifier (e.g. "shard0-primary")
  SHARD_ID              shard identifier      (e.g. "shard0")
  SHARD_RANGE_START     inclusive start of owned range (default 0)
  SHARD_RANGE_END       exclusive end of owned range   (default 2^63)
  SHARD_OWNERSHIP_EPOCH epoch for this range           (default 1)
  PORT                  listen port                    (default 8200)
  CLUSTER_SECRET        ≥32-char shared secret
  ADMIN_TOKEN           ≥32-char admin token
  CLIENT_API_KEY        ≥16-char client API key
  DATA_DIR              base data directory            (default ./data)
"""

from __future__ import annotations

import os

import uvicorn

from docstore.common.config import get_settings
from docstore.common.logging import configure_root_logger
from docstore.shard.app import build_app


def main() -> None:
    settings = get_settings()
    configure_root_logger(settings.LOG_LEVEL)

    node_id = os.environ.get("NODE_ID", "shard-node-default")
    shard_id = os.environ.get("SHARD_ID", "shard0")
    port = int(os.environ.get("PORT", "8200"))

    app = build_app(settings, node_id=node_id, shard_id=shard_id)
    host = os.environ.get("HOST", "127.0.0.1")

    uvicorn.run(app, host=host, port=port, log_config=None)


if __name__ == "__main__":
    main()
