#!/usr/bin/env python3
import copy
import json
import os
import time
import singer
from singer import metrics
from singer import bookmarks
from singer import utils
from singer import metadata
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer)
from dateutil.relativedelta import relativedelta
from tap_criteo.discover import do_discover
from tap_criteo.sync import do_sync


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret"
]

LOGGER = singer.get_logger()

@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = do_discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog =  do_discover()

        do_sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
