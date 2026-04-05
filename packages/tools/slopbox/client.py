"""Elasticsearch client construction from environment variables."""

import logging
import os
import sys

from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Client construction
# ---------------------------------------------------------------------------

def build_client() -> Elasticsearch:
    host = os.environ.get("ES_HOST")
    cloud_id = os.environ.get("ES_CLOUD_ID")
    api_key = os.environ.get("ES_API_KEY")
    username = os.environ.get("ES_USERNAME")
    password = os.environ.get("ES_PASSWORD")

    if not host and not cloud_id:
        logger.error("ES_HOST or ES_CLOUD_ID must be set")
        sys.exit(1)
    if not api_key and not (username and password):
        logger.error("ES_API_KEY or both ES_USERNAME and ES_PASSWORD must be set")
        sys.exit(1)

    kwargs: dict = {}

    if cloud_id:
        kwargs["cloud_id"] = cloud_id
    else:
        kwargs["hosts"] = [host]

    if api_key:
        kwargs["api_key"] = api_key
    else:
        kwargs["basic_auth"] = (username, password)

    return Elasticsearch(**kwargs)
