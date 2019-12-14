import copy
import csv
import io
from dateutil.relativedelta import relativedelta
import singer
from singer import metrics
from singer import bookmarks
from singer import utils
from singer import metadata
from singer import Schema
from singer import Transformer
from tap_criteo.endpoints import (GENERIC_ENDPOINT_MAPPINGS,
                                  SELLER_STATS_REPORT_TYPES,
                                  STATISTICS_REPORT_TYPES)
from tap_criteo.criteo import (create_sdk_client,
                               refresh_auth_token,
                               get_statistics_report)


CSV_DELIMITER = ";"
LOGGER = singer.get_logger()

REPORT_RUN_DATETIME = utils.strftime(utils.now())

def get_attribution_window_bookmark(state, advertiser_ids, stream_name):
    mid_bk_value = bookmarks.get_bookmark(state,
                                          state_key_name(advertiser_ids, stream_name),
                                          "last_attribution_window_date")
    return utils.strptime_with_tz(mid_bk_value) if mid_bk_value else None

def get_start_for_stream(config, state, advertiser_ids, stream_name):
    bk_value = bookmarks.get_bookmark(state,
                                      state_key_name(advertiser_ids, stream_name),
                                      "date")
    bk_start_date = utils.strptime_with_tz(bk_value or config["start_date"])
    return bk_start_date

def apply_conversion_window(config, start_date):
    conversion_window_days = int(config.get("conversion_window_days", "-30"))
    return start_date + relativedelta(days=conversion_window_days)

def get_end_date(config):
    if config.get("end_date"):
        return utils.strptime_with_tz(config.get("end_date"))

    return utils.now()

def state_key_name(advertiser_ids, report_name):
    if advertiser_ids:
        return report_name + "_" + advertiser_ids
    else:
        return report_name

def should_sync(mdata, field):
    inclusion = metadata.get(mdata, field, "inclusion")
    selected = metadata.get(mdata, field, "selected")
    return utils.should_sync_field(inclusion, selected)

def get_fields_to_sync(stream):
    fields = stream.schema.properties # pylint: disable=unsubscriptable-object
    mdata = metadata.to_map(stream.metadata)
    return [field for field in fields if should_sync(mdata, ("properties", field))]

# def filter_fields_by_stream_name(stream_name, fields_to_sync):
#     if stream_name in STATISTICS_REPORT_TYPES:
#         return fields_to_sync
#     elif stream_name in SELLER_STATS_REPORT_TYPES:
#         return fields_to_sync
#     elif stream_name in GENERIC_ENDPOINT_MAPPINGS:
#         return fields_to_sync
#     else:
#         return fields_to_sync

def get_field_list(stream):
    stream.metadata = add_synthetic_keys_to_stream_metadata(stream.metadata)
    field_list = get_fields_to_sync(stream)
    LOGGER.info("Request fields: %s", field_list)
    # field_list = filter_fields_by_stream_name(stream.tap_stream_id, field_list)
    # LOGGER.info("Filtered fields: %s", field_list)
    return field_list

def add_synthetic_keys_to_stream_schema(stream_schema):
    stream_schema.properties["_sdc_report_datetime"] = Schema.from_dict(
        {
            "description": "DateTime of Report Run",
            "type": "string",
            "format" : "date-time"
        }
    )
    stream_schema.properties["_sdc_report_currency"] = Schema.from_dict(
        {
            "description": "Currency of all costs in report",
            "type": "string",
        }
    )
    stream_schema.properties["_sdc_report_ignore_x_device"] = Schema.from_dict(
        {
            "description": "Ignore cross-device data. Also can explicitly set to null for TransactionID ReportType to get all data.",
            "type": "boolean",
        }
    )
    return stream_schema

def add_synthetic_keys_to_stream_metadata(stream_metadata):
    stream_metadata.append({
        "metadata": {"inclusion": "automatic"},
        "breadcrumb": ["properties", "_sdc_report_datetime"]
        }
    )
    stream_metadata.append({
        "metadata": {"inclusion": "automatic"},
        "breadcrumb": ["properties", "_sdc_report_currency"]
        }
    )
    stream_metadata.append({
        "metadata": {"inclusion": "automatic"},
        "breadcrumb": ["properties", "_sdc_report_ignore_x_device"]
        }
    )
    return stream_metadata

def parse_csv_stream(csv_stream):
    # Remove BON
    csv_stream = csv_stream.lstrip("\ufeff")
    # Read a single line into a String, and parse the headers as a CSV
    headers = csv.reader(io.StringIO(csv_stream), delimiter=CSV_DELIMITER)
    header_array = list(headers)[0]

    # Create another CSV reader for the rest of the data
    csv_reader = csv.DictReader(io.StringIO(csv_stream), fieldnames=header_array, delimiter=CSV_DELIMITER)
    next(csv_reader, None)  # Skip header row
    return csv_reader

def sync_statistics_report(config, state, stream, sdk_client):
    advertiser_ids = config.get("advertiser_ids", "")
    mdata = metadata.to_map(stream.metadata)

    stream.schema = add_synthetic_keys_to_stream_schema(stream.schema)

    field_list = get_field_list(stream)

    primary_keys = []
    LOGGER.info("{} primary keys are {}".format(stream.stream, primary_keys))
    singer.write_schema(stream.stream,
                        stream.schema.to_dict(),
                        primary_keys,
                        bookmark_properties=["Day"])

    # If an attribution window sync is interrupted, start where it left off
    start_date = get_attribution_window_bookmark(state, advertiser_ids, stream.stream)
    if start_date is None:
        start_date = apply_conversion_window(
            config,
            get_start_for_stream(config, state, advertiser_ids, stream.stream)
        )

    # According to Criteo's documentation the StatisticsApi only supports between one
    # and three dimensions and at least one metric.
    report_dimensions = [field for field in field_list if metadata.get(mdata, ("properties", field), "behaviour") == "dimension"]
    LOGGER.info("Selected dimensions: %s", report_dimensions)
    if not 0 <= len(report_dimensions) <= 3:
        raise ValueError("%s stream only supports up to 3 selected dimensions" % stream.stream)
    report_metrics = [field for field in field_list if metadata.get(mdata, ("properties", field), "behaviour") == "metric"]
    LOGGER.info("Selected metrics: %s", report_metrics)
    if not len(report_metrics) >= 1:
        raise ValueError("%s stream must have at least 1 selected metric" % stream.stream)

    token = None
    while start_date <= get_end_date(config):
        token = refresh_auth_token(sdk_client, token)
        sync_statistics_for_day(config, state, stream, sdk_client, token, start_date, report_metrics, report_dimensions)
        start_date = start_date+relativedelta(days=1)
        bookmarks.write_bookmark(state,
                                 state_key_name(advertiser_ids, stream.stream),
                                 "last_attribution_window_date",
                                 start_date.strftime(utils.DATETIME_FMT))
        singer.write_state(state)
    bookmarks.clear_bookmark(state,
                             state_key_name(advertiser_ids, stream.stream),
                             "last_attribution_window_date")
    singer.write_state(state)
    LOGGER.info("Done syncing the %s report for advertiser_ids %s", stream.stream, advertiser_ids)

def sync_statistics_for_day(config, state, stream, sdk_client,
                            token, start, report_metrics, report_dimensions): # pylint: disable=too-many-locals
    mdata = metadata.to_map(stream.metadata)
    stats_query = {
        "report_type": stream.tap_stream_id,
        "dimensions": report_dimensions,
        "metrics": report_metrics,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": start.strftime("%Y-%m-%d"),
        "currency": metadata.get(mdata, (), "currency")
    }
    # Filter advertiser_ids if defined in config
    advertiser_ids = config.get("advertiser_ids")
    if advertiser_ids:
        stats_query["advertiserId"] = advertiser_ids
    # Add ignore_x_device if defined in metadata
    ignore_x_device = metadata.get(mdata, (), "ignoreXDevice")
    if ignore_x_device:
        stats_query["ignoreXDevice"] = ignore_x_device

    # Fetch the report as a csv string
    with metrics.http_request_timer(stream.tap_stream_id):
        result = get_statistics_report(sdk_client, stats_query, token=token)

    csv_reader = parse_csv_stream(result)
    with metrics.record_counter(stream.tap_stream_id) as counter:
        time_extracted = utils.now()

        with Transformer() as bumble_bee:
            for row in csv_reader:
                row["_sdc_report_datetime"] = REPORT_RUN_DATETIME
                row["_sdc_report_currency"] = metadata.get(mdata, (), "currency")
                row = bumble_bee.transform(row, stream.schema.to_dict())

                singer.write_record(stream.stream, row, time_extracted=time_extracted)
                counter.increment()

        if start > get_start_for_stream(config, state, advertiser_ids, stream.stream):
            LOGGER.info("updating bookmark: %s > %s", start, get_start_for_stream(config, state, advertiser_ids, stream.stream))
            bookmarks.write_bookmark(state,
                                     state_key_name(advertiser_ids, stream.stream),
                                     "date",
                                     start.strftime(utils.DATETIME_FMT))
            singer.write_state(state)
        else:
            LOGGER.info("not updating bookmark: %s <= %s", start, get_start_for_stream(config, state, advertiser_ids, stream.stream))

        LOGGER.info("Done syncing %s records for the %s report for advertiser_ids %s on %s",
                    counter.value, stream.stream, advertiser_ids, start)

def sync_seller_stats_report(config, state, stream, sdk_client):
    pass

def sync_seller_stats_for_day(config, state, stream, sdk_client,
                              token, start, metrics, report_dimensions):
    pass

def sync_generic_endpoint(config, state, stream, sdk_client):
    pass

def sync_stream(config, state, stream, sdk_client):
    # This bifurcation is real. Generic Endpoints have entirely different
    # performance characteristics and constraints than the Report
    # Endpoints and thus should be kept separate.
    if stream.tap_stream_id in SELLER_STATS_REPORT_TYPES:
        sync_seller_stats_report(config, state, stream, sdk_client)
    elif stream.tap_stream_id in STATISTICS_REPORT_TYPES:
        sync_statistics_report(config, state, stream, sdk_client)
    elif stream.tap_stream_id in GENERIC_ENDPOINT_MAPPINGS:
        sync_generic_endpoint(config, state, stream, sdk_client)
    else:
        raise Exception("Unrecognized tap_stream_id {}".format(stream.tap_stream_id))

def do_sync(config, state, catalog):

    sdk_client = create_sdk_client(config)
    selected_streams = False
    # Loop over streams in catalog
    advertiser_ids = config.get("advertiser_ids", "").split(",")
    if advertiser_ids:
        LOGGER.info("Syncing advertiser IDs %s ...", advertiser_ids)
    else:
        LOGGER.info("Syncing all advertiser IDs ...")

    for stream in catalog.get_selected_streams(state):
        selected_streams = True
        LOGGER.info("Syncing stream: %s", stream.stream)

        sync_stream(config, state, stream, sdk_client)

    if not selected_streams:
        LOGGER.warn("No streams selected")

    if advertiser_ids:
        LOGGER.info("Done syncing advertiser IDs %s ...", advertiser_ids)
    else:
        LOGGER.info("Done syncing all advertiser IDs ...")

    return
