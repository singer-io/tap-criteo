import time
import criteo_marketing
from criteo_marketing.rest import ApiException
import singer


LOGGER = singer.get_logger()

GRANT_TYPE = "client_credentials"  # Other grant types are not available
TIMEZONE = "GMT"  # Hardcoded to GMT since all values will be converted to UTC anyway
FORMAT = "Csv"  # Hardcoded to Csv since all records converted to Singer spec JSON
TOKEN_EXPIRE = 0  # Unix timestamp at which the current auth token expires (seconds)
TOKEN_REFRESH_MARGIN = 60  # Maximum number of seconds before token expiry to get new token

# Return unix timestamp to the nearest second
def get_unixtime():
    return int(time.time())

def create_sdk_client(config):
    LOGGER.info("Creating Criteo Marketing API client with OAuth credentials")
    configuration = criteo_marketing.Configuration(username=config["client_id"],
                                                   password=config["client_secret"])

    # Enable/Disable debug httplib and criteo_marketing packages
    # logging.basicConfig(level=logging.DEBUG)
    # configuration.debug = True

    return criteo_marketing.ApiClient(configuration)

def get_auth_token(client):
    LOGGER.info("Getting OAuth token")
    auth_api = criteo_marketing.AuthenticationApi()
    with singer.metrics.http_request_timer("Authentication"):
        auth_response = auth_api.o_auth2_token_post(client_id=client.configuration.username,
                                                    client_secret=client.configuration.password,
                                                    grant_type=GRANT_TYPE)
    global TOKEN_EXPIRE
    TOKEN_EXPIRE = get_unixtime() + auth_response.expires_in
    # Token type is always "BEARER"
    return auth_response.token_type + " " + auth_response.access_token

def refresh_auth_token(client, token):
    time_to_expire = TOKEN_EXPIRE - TOKEN_REFRESH_MARGIN
    if time_to_expire <= get_unixtime():
        # Don't log time to expire on if no token provided
        if token:
            LOGGER.info("Token expires in %d" % time_to_expire)
        return get_auth_token(client)

    return token

def exception_is_4xx(exception):
    """Returns True if exception is in the 4xx range."""
    if not hasattr(exception, "status"):
        return False

    return 400 <= exception.status < 500

@singer.utils.backoff((ApiException,), exception_is_4xx)
def get_statistics_report(client, stats_query, token=None):
    token = token or get_auth_token(client)
    defaults = {
        "format": FORMAT,
        "timezone": TIMEZONE
    }
    stats_query.update(defaults)
    stats_api = criteo_marketing.StatisticsApi(client)
    stats_query_message = criteo_marketing.StatsQueryMessageEx(**stats_query)

    return stats_api.get_stats(token, stats_query_message)
