frozenset([
    # "AdvertisersApi",         --- no need to support overlay with other endpoints
    "AudiencesApi",
    # "AuthenticationApi",      --- Only for auth
    "BudgetsApi",
    "CampaignsApi",
    "CategoriesApi",
    "PortfolioApi",
    # "PublishersApi",          --- annoying to support requires a temp file to be downloaded
    # "SellersApi",             --- replaced by SellersV2Api endpoint
    "SellersV2Api",
    "SellersV2StatsApi",
    "StatisticsApi"
])
GENERIC_ENDPOINT_MAPPINGS = {
    "AdvertiserInfo",
    "Audiences",
    "Budgets",
    "CampaignBids",
    "Campaigns",
    "Categories",
    "Portfolio",
    "SellerBudgets",
    "SellerCampaigns",
    "Sellers",
}
SELLER_STATS_REPORT_TYPES =[
    "CampaignStats",
    "SellerCampaignStats",
    "SellerStats"
]
STATISTICS_REPORT_TYPES = [
    "CampaignPerformance",
    "FacebookDPA",
    "TransactionID"
]
