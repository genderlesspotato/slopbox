"""Shared Elasticsearch client factory for Temporal activities.

Constructed inline from environment variables so the worker image does not
need to depend on ``slopbox-tools`` (which pulls ``kubernetes``, ``rich``
etc.).  Activities call ``build_es_client()`` at the top of their body and
let the returned client go out of scope when the activity returns.
"""

from __future__ import annotations

import os

from elasticsearch import Elasticsearch
from temporalio.exceptions import ApplicationError


def build_es_client() -> Elasticsearch:
    """Construct an Elasticsearch client from environment variables.

    Recognised variables:
      * ``ES_HOST``     — endpoint URL (mutually exclusive with ``ES_CLOUD_ID``)
      * ``ES_CLOUD_ID`` — Elastic Cloud id (takes precedence over ``ES_HOST``)
      * ``ES_API_KEY``  — base64 ``id:key`` string (preferred)
      * ``ES_USERNAME`` / ``ES_PASSWORD`` — basic auth fallback

    Raises ``ApplicationError(non_retryable=True)`` when neither
    ``ES_HOST`` nor ``ES_CLOUD_ID`` is set — retrying won't fix a missing env
    var.
    """
    host = os.environ.get("ES_HOST")
    cloud_id = os.environ.get("ES_CLOUD_ID")
    api_key = os.environ.get("ES_API_KEY")
    username = os.environ.get("ES_USERNAME")
    password = os.environ.get("ES_PASSWORD")

    kwargs: dict = {}
    if api_key:
        kwargs["api_key"] = api_key
    elif username and password:
        kwargs["basic_auth"] = (username, password)

    if cloud_id:
        return Elasticsearch(cloud_id=cloud_id, **kwargs)
    if host:
        return Elasticsearch(host, **kwargs)
    raise ApplicationError(
        "ES_HOST or ES_CLOUD_ID must be set",
        non_retryable=True,
    )
