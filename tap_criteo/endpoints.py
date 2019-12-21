"""Constants which describe Criteo endpoints."""
frozenset(
    [
        # "AdvertisersApi",     --- overlap with other endpoints
        "AudiencesApi",
        # "AuthenticationApi",  --- Only for auth
        "BudgetsApi",
        "CampaignsApi",
        "CategoriesApi",
        "PortfolioApi",
        # "PublishersApi",      --- requires a temp file to be downloaded
        # "SellersApi",         --- replaced by SellersV2Api endpoint
        "SellersV2Api",
        "SellersV2StatsApi",
        "StatisticsApi",
    ]
)
GENERIC_ENDPOINT_MAPPINGS = {
    "AdvertiserInfo": {"module": "SellersV2Api", "method": "get_advertisers"},
    "Audiences": {"module": "AudiencesApi", "method": "get_audiences"},
    "Budgets": {"module": "BudgetsApi", "method": "get"},
    "CampaignBids": {"module": "CampaignsApi", "method": "get_bids"},
    "Campaigns": {"module": "CampaignsApi", "method": "get_campaigns"},
    "Categories": {"module": "CategoriesApi", "method": "get_categories"},
    "Portfolio": {"module": "PortfolioApi", "method": "get_portfolio"},
    "SellerBudgets": {
        "module": "SellersV2Api",
        "method": "get_seller_budgets",
    },
    "SellerCampaigns": {
        "module": "SellersV2Api",
        "method": "get_seller_campaigns",
    },
    "Sellers": {"module": "SellersV2Api", "method": "get_sellers"},
}
SELLER_STATS_REPORT_TYPES = [
    "CampaignStats",
    "SellerCampaignStats",
    "SellerStats",
]
STATISTICS_REPORT_TYPES = [
    "CampaignPerformance",
    "FacebookDPA",
    "TransactionID",
]
