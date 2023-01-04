import os

import pandas as pd

from libraryImports import *
from createCommunity import *


def calculateAcceptanceRate(memberID, timestamp, memberCommunityData, transactionData, poolOrMarket):
    if poolOrMarket == 'pool':
        totalAllocatedEnergy = abs(sum(memberCommunityData['energyAllocatedToPools (kwh)']))

        matchedEnergy = sum(transactionData[transactionData['timestamp'] == timestamp]['energyTraded (kwh)'])
    else:
        totalAllocatedEnergy = memberCommunityData.loc[0, 'tradeableEnergy (kwh)']
        matchedEnergy = totalAllocatedEnergy - memberCommunityData.loc[0, 'leftoverEnergy (kwh)']

    if totalAllocatedEnergy != 0:
        acceptanceRate = matchedEnergy / totalAllocatedEnergy * 100
    else:
        acceptanceRate = 0

    return acceptanceRate

def calculateSelfSufficiencyAndConsumption(communityLog):
    allMemberIDs = communityLog['memberID'].unique()

    ## Self-sufficiency = (Total demand in community - external import) / total demand in community
    consumerOnlyData = communityLog[communityLog['tradeableEnergy (kwh)'] < 0]
    leftoverDemand = sum(consumerOnlyData['leftoverEnergy (kwh)'])
    totalDemand = sum(consumerOnlyData['tradeableEnergy (kwh)'])
    if totalDemand != 0:
        selfSufficiency = (totalDemand - leftoverDemand) / totalDemand * 100
    else:
        selfSufficiency = 0
    #print('Self-sufficency: {}'.format(selfSufficiency))

    ## Self-consumption = (KW power generated in community - total export) / KW power generated in community
    producerOnlyData = communityLog[communityLog['tradeableEnergy (kwh)'] > 0]
    leftoverSupply = sum(producerOnlyData['leftoverEnergy (kwh)'])
    totalSupply = sum(producerOnlyData['tradeableEnergy (kwh)'])

    if totalSupply > 0:
        selfConsumption = (totalSupply - leftoverSupply) / totalSupply * 100
    else:
        selfConsumption = 0


    #production to consumption ratio
    if totalDemand != 0:
        prodToConsRatio = totalSupply / totalDemand * (-1)
    else:
        prodToConsRatio = 0

    return selfSufficiency, selfConsumption, prodToConsRatio

def extractEnergyAndRoleForOneUser(transactionData, timestamp):
    timepointData = transactionData[transactionData['timestamp'] == timestamp]

    role = -1
    energyTraded = 0
    tradingMoney = 0

    if len(timepointData) > 0:
        if timepointData['allocatedEnergy (kwh)'].values[0] > 0:
            role = 1
        elif timepointData['allocatedEnergy (kwh)'].values[0] < 0:
            role = 0
        energyTraded = timepointData['energyTraded (kwh)'].values[0]
        tradingMoney = timepointData['totalPrice (ct)'].values[0]


    return energyTraded, tradingMoney, role

def calculateAllMetricsPerRound(communityLog, roundLogDir, calculateAcceptance, poolOrMarket, marketPrice, allMemberIDs):
    acceptanceRates = []
    if os.path.exists(os.path.join(roundLogDir, f'performanceMetrics_{poolOrMarket}.csv')):
        communityPerformanceMetricsTable = pd.read_csv(os.path.join(roundLogDir, f'performanceMetrics_{poolOrMarket}.csv'))
        if calculateAcceptance:
            acceptanceRates = pd.read_csv(os.path.join(roundLogDir, f'acceptanceRates_{poolOrMarket}.csv'))
    else:
        communityPerformanceMetricsTable = pd.DataFrame()
        acceptanceRates = pd.DataFrame()


    simulationRound = communityLog['simRound'][0]
    timestamp = communityLog['timestamp'][0]

    selfSufficiency, selfConsumption, PCR = calculateSelfSufficiencyAndConsumption(communityLog)



    totalTradedEnergyProd, totalTradedEnergyCons = 0, 0
    totalTradingMoneyProd, totalTradingMoneyCons  = 0, 0


    ## Acceptance rate (per person) = sum of matched energy in pools / allocated tradable energy to the pools(preferences)
    for memberID in allMemberIDs:
        transactionData = pd.read_csv(os.path.join(roundLogDir, '{}_transactions.csv'.format(memberID)))
        memberCommunityData = communityLog[communityLog['memberID'] == memberID].reset_index()

        tradedEnergy, tradingMoney, memberRole = extractEnergyAndRoleForOneUser(transactionData, timestamp)

        if memberRole == 0:
            totalTradedEnergyCons += tradedEnergy
            totalTradingMoneyCons += tradingMoney
        elif memberRole == 1:
            totalTradedEnergyProd += tradedEnergy
            totalTradingMoneyProd += tradingMoney

        if calculateAcceptance:
            memberAcceptanceRate = calculateAcceptanceRate(memberID, timestamp, memberCommunityData, transactionData, poolOrMarket)
            memberAcceptanceRateDF = pd.DataFrame(
                {'Round': [simulationRound], 'Timestamp': timestamp, 'memberID': [memberID], 'acceptanceRate': [memberAcceptanceRate]})
            acceptanceRates = pd.concat([acceptanceRates, memberAcceptanceRateDF])
    if calculateAcceptance:
        acceptanceRates.to_csv(os.path.join(roundLogDir, f'acceptanceRates_{poolOrMarket}.csv'), index=False)

    if totalTradedEnergyProd > 0:
        marketElectricityPriceProd = totalTradedEnergyProd * marketPrice
        shareMarketSavingsProd = (marketElectricityPriceProd - totalTradingMoneyProd) / marketElectricityPriceProd * 100
    else:
        shareMarketSavingsProd = 0

    if totalTradedEnergyCons > 0:
        marketElectricityPriceCons = totalTradedEnergyCons * marketPrice
        shareMarketSavingsCons = (marketElectricityPriceCons - totalTradingMoneyCons) / marketElectricityPriceCons * 100
    else:
        shareMarketSavingsCons = 0

    tmp = pd.DataFrame({'Round': [simulationRound], 'Timestamp': [timestamp],  'Self-sufficiency': [selfSufficiency], 'Self-consumption': [selfConsumption], 'PCR': [PCR], 'shareMarketSavingsProd': [shareMarketSavingsProd], 'shareMarketSavingsCons': [shareMarketSavingsCons]})
    communityPerformanceMetricsTable = pd.concat([communityPerformanceMetricsTable, tmp])

    communityPerformanceMetricsTable.to_csv(os.path.join(roundLogDir, f'performanceMetrics_{poolOrMarket}.csv'), index=False)



    return communityPerformanceMetricsTable, acceptanceRates

def calculateMetricsPerRoundForEachPool(timestamp, simulationRound, roundLogDir, allMemberIDs, calculateAcceptance, poolName, marketPrice):
    acceptanceRates = []
    if os.path.exists(os.path.join(roundLogDir, f'performanceMetrics_{poolName}.csv')):
        communityPerformanceMetricsTable = pd.read_csv(os.path.join(roundLogDir, f'performanceMetrics_{poolName}.csv'))
        if calculateAcceptance:
            acceptanceRates = pd.read_csv(os.path.join(roundLogDir, f'acceptanceRates_{poolName}.csv'))
    else:
        communityPerformanceMetricsTable = pd.DataFrame()
        acceptanceRates = pd.DataFrame()


    # simulationRound = communityLog['simRound'][0]
    # timestamp = communityLog['timestamp'][0]


    totalTradedEnergyProd, totalTradedEnergyCons = 0, 0
    totalTradingMoneyProd, totalTradingMoneyCons  = 0, 0

    ## Acceptance rate (per person) = sum of matched energy in pools / allocated tradable energy to the pools(preferences)
    for memberID in allMemberIDs:
        transactionData = pd.read_csv(os.path.join(roundLogDir, '{}_transactions.csv'.format(memberID)))

        poolTransData = transactionData[transactionData['pool'] == poolName]
        tradedEnergy, tradingMoney, memberRole = extractEnergyAndRoleForOneUser(poolTransData, timestamp)

        if memberRole == 0:
            totalTradedEnergyCons += tradedEnergy
            totalTradingMoneyCons += tradingMoney
        elif memberRole == 1:
            totalTradedEnergyProd += tradedEnergy
            totalTradingMoneyProd += tradingMoney

        if calculateAcceptance:
            memberCommunityData = communityLog[communityLog['memberID'] == memberID].reset_index()
            memberAcceptanceRate = calculateAcceptanceRate(memberID, timestamp, memberCommunityData, transactionData, 'pool')
            memberAcceptanceRateDF = pd.DataFrame(
                {'Round': [simulationRound], 'Timestamp': timestamp, 'memberID': [memberID], 'acceptanceRate': [memberAcceptanceRate]})
            acceptanceRates = pd.concat([acceptanceRates, memberAcceptanceRateDF])
    if calculateAcceptance:
        acceptanceRates.to_csv(os.path.join(roundLogDir, f'acceptanceRates_{poolName}.csv'), index=False)

    if totalTradedEnergyProd > 0:
        marketElectricityPriceProd = totalTradedEnergyProd * marketPrice
        shareMarketSavingsProd = (marketElectricityPriceProd - totalTradingMoneyProd) / marketElectricityPriceProd * 100
    else:
        shareMarketSavingsProd = 0

    if totalTradedEnergyCons > 0:
        marketElectricityPriceCons = totalTradedEnergyCons * marketPrice
        shareMarketSavingsCons = (marketElectricityPriceCons - totalTradingMoneyCons) / marketElectricityPriceCons * 100
    else:
        shareMarketSavingsCons = 0

    tmp = pd.DataFrame({'Round': [simulationRound], 'Timestamp': [timestamp],  'shareMarketSavingsProd': [shareMarketSavingsProd], 'shareMarketSavingsCons': [shareMarketSavingsCons]})
    communityPerformanceMetricsTable = pd.concat([communityPerformanceMetricsTable, tmp])

    communityPerformanceMetricsTable.to_csv(os.path.join(roundLogDir, f'performanceMetrics_{poolName}.csv'), index=False)



    return communityPerformanceMetricsTable, acceptanceRates



