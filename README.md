# tap-criteo

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from the Criteo Marketing API
- Extracts the following resources from Criteo for a one or more advertisers:
  - [Audiences](https://api.criteo.com/marketing/swagger/ui/index#!/Audiences/Audiences_GetAudiences)
  - [AdvertiserInfo](https://api.criteo.com/marketing/swagger/ui/index#!/SellersV2/SellersV2_GetAdvertisers)
  - [Budgets](https://api.criteo.com/marketing/swagger/ui/index#!/Budgets/Budgets_Get)
  - [Campaigns](https://api.criteo.com/marketing/swagger/ui/index#!/Campaigns/Campaigns_GetCampaigns)
  - [CampaignBids](https://api.criteo.com/marketing/swagger/ui/index#!/Campaigns/Campaigns_GetBids)
  - [CampaignPerformance](https://api.criteo.com/marketing/swagger/ui/index#!/Statistics/Statistics_GetStats)
  - [CampaignStats](https://api.criteo.com/marketing/swagger/ui/index#!/SellersV2Stats/SellersV2Stats_Campaigns)
  - [Categories](https://api.criteo.com/marketing/swagger/ui/index#!/Categories/Categories_GetCategories)
  - [FacebookDPA](https://api.criteo.com/marketing/swagger/ui/index#!/Statistics/Statistics_GetStats)
  - [Portfolio](https://api.criteo.com/marketing/swagger/ui/index#!/Portfolio/Portfolio_GetPortfolio)
  - [SellerBudgets](https://api.criteo.com/marketing/swagger/ui/index#!/SellersV2/SellersV2_GetSellerBudgets)
  - [SellerCampaigns](https://api.criteo.com/marketing/swagger/ui/index#!/SellersV2/SellersV2_GetSellerCampaigns)
  - [SellerCampaignStats](https://api.criteo.com/marketing/swagger/ui/index#!/SellersV2Stats/SellersV2Stats_SellerCampaigns)
  - [Sellers](https://api.criteo.com/marketing/swagger/ui/index#!/SellersV2/SellersV2_GetSellers)
  - [SellerStats](https://api.criteo.com/marketing/swagger/ui/index#!/SellersV2Stats/SellersV2Stats_Sellers)
  - [TransactionID](https://api.criteo.com/marketing/swagger/ui/index#!/Statistics/Statistics_GetStats)


- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

### Install

We recommend using a virtualenv:

```bash
> virtualenv -p python3 venv
> source venv/bin/activate
> pip install tap-criteo
```

### Get Access to the Criteo Marketing API

To use the Criteo Marketing API, you must create an API user.
https://support.criteo.com/s/article?article=360001285145-Getting-Started

### Create the config file

The Criteo Tap will use the `client_id` and `client_secret` obtained from the previous step. Additionally you will need:

  **start_date** - an initial date for the Tap to extract Criteo data

The following is an example of the minimum required configuration

```json
{"client_id": "",
 "client_secret": "",
 "start_date": ""}
```

Optionally, you may define the following keys in the configuration

  **end_date** - used to only pull data up to a given date  
  **user_agent** - used in requests made to the Criteo Marketing API  
  **advertiser_ids** - A comma-separated list of Criteo advertiser IDs which you wish to replicate data from. If not defined then all avertiser IDs will be replicated.  

### Create a catalog file

The catalog file will indicate what streams and fields to replicate from the Criteo Marketing API. The Tap takes advantage of the Singer best practices for [schema discovery and catalog selection](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#the-catalog).

### [Optional] Create the initial state file

You can provide JSON file that contains a date for the streams to force the application to only fetch data newer than those dates. If you omit the file it will fetch all data for the selected streams.

```json
{"campaign_performance_12345":"2017-01-01T00:00:00Z",
 "CampaignPerformance":"2017-01-01T00:00:00Z",
 "FacebookDPA":"2017-01-01T00:00:00Z"}
```

### Run the Tap

`tap-criteo -c config.json --catalog catalog.json -s state.json`

## Metadata Reference

tap-criteo uses some custom metadata keys for some endpoints:

### Statistics (CampaignPerformance, FacebookDPA, TransactionID)

#### Stream metadata

* `currency` - The currency to be used in the report. Three-letter capitals. For a list of possible values, please see the full documentation. Defaults to USD.
* `ignoreXDevice` - Ignore cross-device data. Also can explicitly set to null for TransactionID ReportType to get all data. Defaults to false.

#### Field metadata

* `behaviour` - Either metric or dimension. As defined by Criteo's [documentation](https://support.criteo.com/s/article?article=360001362485-Retrieve-statistics)
* `fieldExclusions` - Indicates which other fields may not be selected when this field is selected. If you invoke the tap with selections that violate fieldExclusion rules, the tap will fail.

### SellersV2Stats (SellerStats, CampaignStats, SellerCampaignStats)

#### Stream metadata

* `clickAttributionPolicy` - Specify the click attribution policy for salesUnits, revenue, CR, CPO, COS, and ROAS (`sameSeller` or `anySeller`)

#### Field metadata

* `behaviour` - Either metric or dimension. As defined by Criteo's [documentation](https://api.criteo.com/marketing/swagger/ui/index#/SellersV2Stats)
* `fieldExclusions` - Indicates which other fields may not be selected when this field is selected. If you invoke the tap with selections that violate fieldExclusion rules, the tap will fail.

---

Copyright &copy; 2019 Stitch
