#################### Initial Declarations ####################
# region
# importing packages
# region
import os
import asyncio
import pandas as pd
import pyscope as ps
import pyscopesubmit as pss
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 100)


# endregion

# declaring the variables
# region
session = pss.CfeScopeJobManagementClient.create_interactive_session(
    username='sdrona@microsoft.com')

vcATS = 'https://cosmos08.osdinfra.net/cosmos/bingads.algo.adquality'
vcMP = 'https://cosmos08.osdinfra.net/cosmos/bingads.marketplace.VC1'

localPath_folder = os.getcwd()
cosmosOutputPath_folder = '/local/users/sdrona/LowCoverageAnalysis/pyScopeDemo/'
cosmosOutputPath_folder_full = 'https://cosmos08.osdinfra.net/cosmos/bingads.algo.adquality' + \
    cosmosOutputPath_folder

# cosmosOutputPath_folder = '/local/users/sdrona/LowCoverageAnalysis/vAug24/'
# cosmosOutputPath_folder_full = 'https://cosmos08.osdinfra.net/cosmos/bingads.algo.adquality' + \
#     cosmosOutputPath_folder

start_date = '2024-07-01'
end_date = '2024-08-01'

target_countryNames = ['australia', 'india', 'turkey', 'poland', 'mexico',
                       'venezuela', 'saudi arabia', 'united arab emirates', 'south africa']
target_countryCodes = ['AU', 'IN', 'TR', 'PL', 'MX', 'VE', 'SA', 'AE', 'ZA']

sample_size = 30

# endregion

# defining the functions
# region


async def create_local_data_from_ss(cosmosFilePath, localFilePath, sample_size):
    sampledData = asyncio.create_task(
        session.sample_sstream(cosmosFilePath,
                               localFilePath,
                               sample_size,
                               override=True))
    await sampledData
    sampledData = session.read_sstream(cosmosFilePath, localFilePath)
    return sampledData


async def download_cosmos_csv(cosmosFilePath, localFilePath):
    sampledData = asyncio.create_task(
        session.download_stream(cosmosFilePath,
                                localFilePath,
                                override=True))
    await sampledData
    return print('Downloaded the file to local')


async def submit_cosmos_job(outputs, script_dir, vc, priority):
    cfe_client = session.get_job_client()
    cfe_scope_job = await cfe_client.submit_to_cfe(outputs=outputs,
                                                   script_dir=script_dir,
                                                   vc=vc,
                                                   priority=priority)
    return cfe_scope_job.url


# def download_cosmos_ss_as_csv(cosmosFilePath, localFilePath):
#     import subprocess
#     powershell_command = f'Export-CosmosStructuredStreamToCSV "{cosmosFilePath}" "{localFilePath}"'
#     subprocess.call(['powershell.exe', powershell_command], shell=True)
#     return print('Downloaded the file to local')


# endregion

#################### Clicks & Domains Data ####################
# region

# Clicks Data by Country and Domain
# region
# consolidating CUV Clicks data

df = await create_local_data_from_ss('https://cosmos08.osdinfra.net/cosmos/bingads.algo.adquality/shares/bingAds.BI.OI/AdsOI/DemandMetrics/Data/ClickShare/GABT/Bing/BingIEClicks_2024-07-15_2024-07-21.ss',
                                     localPath_folder + 'cuvLogs/BingIEClicks_2024-07-15_2024-07-21.ss',
                                     sample_size)
df.dtypes

cuvConsolidatedClicks_dtypes = session.get_sstream_dtypes(
    'https://cosmos08.osdinfra.net/cosmos/bingads.algo.adquality/shares/bingAds.BI.OI/AdsOI/DemandMetrics/Data/ClickShare/GABT/Bing/BingIEClicks_2024-07-15_2024-07-21.ss')

cuvConsolidatedClicks = session.read_sstream_fileset(
    filepath='https://cosmos08.osdinfra.net/cosmos/bingads.algo.adquality/shares/bingAds.BI.OI/AdsOI/DemandMetrics/Data/ClickShare/GABT/Bing/BingIEClicks_2024-07-{*}.ss',
    dtypes=cuvConsolidatedClicks_dtypes,
    local_data_filepath=localPath_folder + 'cuvLogs/{*}.ss')

queries_g = cuvConsolidatedClicks[(cuvConsolidatedClicks['Country'].isin(target_countryNames)) &
                                  (cuvConsolidatedClicks['LogDate'] >= pd.to_datetime(start_date)) &
                                  (cuvConsolidatedClicks['LogDate'] < pd.to_datetime(end_date)) &
                                  (cuvConsolidatedClicks['Query_Normalized'].notnull()) &
                                  (cuvConsolidatedClicks['Advertiser_new'] != 'blank')][[
                                      'Query_Normalized', 'Country', 'Advertiser_new', 'Click']].groupby(
    ['Query_Normalized', 'Country', 'Advertiser_new']).agg({'Click': 'sum'})

queries_g.columns = ['QueryPhrase', 'Country', 'domain_g', 'Clicks_g']

queries_g['CountryCode'] = queries_g['Country'].apply(expr='lambda x: country_map[x]',
                                                      prologue="country_map = {'australia': 'AU', 'india': 'IN', 'mexico': 'MX', 'venezuela': 'VE', 'turkey': 'TR', 'south africa': 'ZA', 'saudi arabia': 'SA', 'united arab emirates': 'AE', 'poland': 'PL'}",
                                                      dtype='String')

queries_g_ss = queries_g.to_sstream(
    cosmosOutputPath_folder_full + 'queries_g_sea.ss')

await submit_cosmos_job(outputs=(queries_g_ss,),
                        script_dir=localPath_folder,
                        vc=vcATS,
                        priority=950)

# endregion

# Bing Clicks Data by Country and Domain
# region
MonetizationFactView = "/shares/adCenter.BICore.SubjectArea/SubjectArea/Monetization/views/PROD/MonetizationFacts.view"

MVData = session.read_scope_view(
    view_path=MonetizationFactView,
    dtypes={
        'QueryPhrase': ps.String,
        'RGUID': ps.String,
        'IsPageViewRecord': ps.bool,
        'IsAdImpressionRecord': ps.bool,
        'IsClickRecord': ps.bool,
        'UniqueAdListingId': ps.int64,
        'AdId': ps.int64,
        'FraudQualityBand': ps.int8,
        'IncomingPublisherWebSiteCountry': ps.String,
        'MediumId': ps.int64,
        'RelatedToAccountId': ps.int32,
        'MarketplaceClassificationId': ps.int8,
        'DeviceTypeId': ps.int8,
        'FormCodeClassification': ps.int8,
        'RawSearchPages': ps.int64,
        'RawBiddedSearchPages': ps.int64,
        'IsAdImpressionRecord': ps.bool,
        'ImpressionCnt': ps.int64,
        'IsClickRecord': ps.bool,
        'ClickCnt': ps.int64,
        'AmountChargedUSDMonthlyExchangeRt': ps.float64,
        'AdvertiserAccountId': ps.int64,
        'CustomerId': ps.int64
    },
    params={
        'StartDateTimeUtc': pd.to_datetime(start_date),
        'EndDateTimeUtc': pd.to_datetime(end_date),
        'ReadAdImpressions': False,
        'ReadPageViews': True,
        'ReadConversions': False,
        'ReadClicks': True,
        'ReadUETConversions': False,
        'Traffic': "PaidSearch",
        'FraudType': "NonFraud"
    },
)

MVData = MVData[((MVData['IsPageViewRecord'] == True) | (MVData['IsAdImpressionRecord'] == True) | (MVData['IsClickRecord'] == True)) &
                (MVData['FraudQualityBand'] > 1) &
                (MVData['IncomingPublisherWebSiteCountry'].str.upper().isin(target_countryCodes)) &
                (MVData['MediumId'].isin([1, 3])) &
                (MVData['RelatedToAccountId'] == 1004) &
                (MVData['MarketplaceClassificationId'] == 1) &
                (MVData['DeviceTypeId'].isin([1, 2, 4])) &
                (MVData['FormCodeClassification'] == 0) &
                (MVData['QueryPhrase'].notnull())]

queries_rguids = MVData[[
    'QueryPhrase', 'IncomingPublisherWebSiteCountry', 'RGUID']].drop_duplicates()

queries_srpvs = MVData.groupby(['QueryPhrase',
                                'IncomingPublisherWebSiteCountry']).agg({
                                    'RawSearchPages': 'sum',
                                    'RawBiddedSearchPages': 'sum',
                                    'ImpressionCnt': 'sum',
                                    'ClickCnt': 'sum'})

queries_ads = MVData.groupby(['QueryPhrase',
                              'IncomingPublisherWebSiteCountry',
                              'CustomerId',
                              'AdvertiserAccountId',
                              'AdId',
                              'UniqueAdListingId']).agg({
                                  'ImpressionCnt': 'sum',
                                  'ClickCnt': 'sum'})

queries_ads['AdId'] = queries_ads['AdId'].astype('Int64')

queries_rguids_ss = queries_rguids.to_sstream(
    cosmosOutputPath_folder_full + 'queries_rguids_sea.ss')
queries_srpvs_ss = queries_srpvs.to_sstream(
    cosmosOutputPath_folder_full + 'queries_srpvs_sea.ss')
queries_ads_ss = queries_ads.to_sstream(
    cosmosOutputPath_folder_full + 'queries_ads_sea.ss')

await submit_cosmos_job(outputs=(queries_rguids_ss, queries_srpvs_ss, queries_ads_ss),
                        script_dir=localPath_folder,
                        vc=vcATS,
                        priority=950)

# endregion