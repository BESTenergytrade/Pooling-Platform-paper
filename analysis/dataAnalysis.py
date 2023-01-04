import os

import pandas as pd

from libraryImports import *
from createCommunity import *
from performanceMetrics import *
from fakeBiddingAgent import marketPrice
from auxFunctions import *
from globalVars import *
#
# analysisFolder = f'/testLogs/Analysis'
# if not os.path.exists(analysisFolder):
#     os.mkdir(analysisFolder)

communityConditions = ['cons5_pros15', 'cons10_pros10', 'cons15_pros5']
tradingConditions = ['poolingAndMarket', 'poolingOnly', 'marketOnly']

tradingConditionsStats = {'poolingAndMarket': {}, 'poolingOnly': {}, 'marketOnly': {}}
testingMode = 'poolingAndMarket'

def createCommunityConditionsTables():
    for communityCondition in communityConditions:
        tcTableColumns = {f'SS_Pool_{communityCondition}': [-1]*noSimulationRounds, f'SC_Pool_{communityCondition}': [-1]*noSimulationRounds, f'SS_Market_{communityCondition}': [-1]*noSimulationRounds, f'SC_Market_{communityCondition}': [-1]*noSimulationRounds}
        ccTable = {'Round': [-1]*noSimulationRounds}
        for tradingCondition in tradingConditions:
            poolsLogDirRoot = f'testLogs/{communityCondition}/{tradingCondition}'
            ccTableColumns = {f'SS_Pool_{tradingCondition}': [-1] * noSimulationRounds, f'SC_Pool_{tradingCondition}': [-1] * noSimulationRounds, f'SS_Market_{tradingCondition}': [-1] * noSimulationRounds, f'SC_Market_{tradingCondition}': [-1] * noSimulationRounds}
            ccTable.update(ccTableColumns)

            tradingConditionsStats[tradingCondition].update(tcTableColumns)
            for simulationRound in range(1, (noSimulationRounds+1)):
                poolsLogDir = os.path.join('../', poolsLogDirRoot, 'Simround_{}'.format(simulationRound))
                index = simulationRound - 1
                ccTable['Round'][index] = simulationRound

                if (tradingCondition != 'marketOnly') :
                    communityMetricsPool = pd.read_csv(os.path.join(poolsLogDir, 'performanceMetrics_pool.csv'))
                    selfSufficiencyPool = communityMetricsPool['Self-sufficiency'].mean()
                    selfConsumptionPool = communityMetricsPool['Self-consumption'].mean()
                    ccTable[f'SS_Pool_{tradingCondition}'][index] = selfSufficiencyPool
                    ccTable[f'SC_Pool_{tradingCondition}'][index] = selfConsumptionPool

                    tradingConditionsStats[tradingCondition][f'SS_Pool_{communityCondition}'][index] = selfSufficiencyPool
                    tradingConditionsStats[tradingCondition][f'SC_Pool_{communityCondition}'][index] = selfConsumptionPool


                if (tradingCondition != 'poolingOnly'):
                    mLogPath = os.path.join(poolsLogDir, 'performanceMetrics_market.csv')
                    if os.path.exists(mLogPath):
                        communityMetricsMarket = pd.read_csv(mLogPath)
                    else:
                        communityMetricsMarket = pd.DataFrame()

                    selfSufficiencyMarket = communityMetricsMarket['Self-sufficiency'].mean()
                    selfConsumptionMarket = communityMetricsMarket['Self-consumption'].mean()
                    ccTable[f'SS_Market_{tradingCondition}'][index] = selfSufficiencyMarket
                    ccTable[f'SC_Market_{tradingCondition}'][index] = selfConsumptionMarket

                    tradingConditionsStats[tradingCondition][f'SS_Market_{communityCondition}'][index] = selfSufficiencyMarket
                    tradingConditionsStats[tradingCondition][f'SC_Market_{communityCondition}'][index] = selfConsumptionMarket

        ccTableFileName = f'{communityCondition}_tradingConditionStats.csv'
        ccTableFilePath = os.path.join(analysisPath, ccTableFileName)
        ccTable = pd.DataFrame(ccTable)
        ccTable.to_csv(ccTableFilePath)


# for tradingCondition in tradingConditions:
#     tcTableFileName = f'{tradingCondition}_communityConditionStats.csv'
#     tcTableFilePath = os.path.join(analysisFolder, tcTableFileName)
#     tcTable = pd.DataFrame(tradingConditionsStats[tradingCondition])
#     tcTable.to_csv(tcTableFilePath)

def createTradingConditionsTables():
    for tradingCondition in tradingConditions:
        ccTable = {'Round': [-1]*noSimulationRounds}
        for communityCondition in communityConditions:
            poolsLogDirRoot = f'testLogs/{communityCondition}/{tradingCondition}'
            ccTableColumns = {f'SS_Pool_{communityCondition}': [-1] * noSimulationRounds, f'SC_Pool_{communityCondition}': [-1] * noSimulationRounds, f'SS_Market_{communityCondition}': [-1] * noSimulationRounds, f'SC_Market_{communityCondition}': [-1] * noSimulationRounds}
            ccTable.update(ccTableColumns)

            for simulationRound in range(1, (noSimulationRounds+1)):
                poolsLogDir = os.path.join('../', poolsLogDirRoot, 'Simround_{}'.format(simulationRound))
                index = simulationRound - 1
                ccTable['Round'][index] = simulationRound

                if (tradingCondition != 'marketOnly') :
                    communityMetricsPool = pd.read_csv(os.path.join(poolsLogDir, 'performanceMetrics_pool.csv'))
                    selfSufficiencyPool = communityMetricsPool['Self-sufficiency'].mean()
                    selfConsumptionPool = communityMetricsPool['Self-consumption'].mean()
                    ccTable[f'SS_Pool_{communityCondition}'][index] = selfSufficiencyPool
                    ccTable[f'SC_Pool_{communityCondition}'][index] = selfConsumptionPool


                if (tradingCondition != 'poolingOnly'):
                    communityMetricsMarket = pd.read_csv(os.path.join(poolsLogDir, 'performanceMetrics_market.csv'))
                    selfSufficiencyMarket = communityMetricsMarket['Self-sufficiency'].mean()
                    selfConsumptionMarket = communityMetricsMarket['Self-consumption'].mean()
                    ccTable[f'SS_Market_{communityCondition}'][index] = selfSufficiencyMarket
                    ccTable[f'SC_Market_{communityCondition}'][index] = selfConsumptionMarket


        ccTableFileName = f'{tradingCondition}_communityConditionStats.csv'
        ccTableFilePath = os.path.join(analysisPath, ccTableFileName)
        ccTable = pd.DataFrame(ccTable)
        ccTable.to_csv(ccTableFilePath)


def recalculatePerformanceMetrics(by_pool, clearPrevData=False):
    if clearPrevData:
        cleanAllPreviousData('performanceMetrics_pool', communityConditions, tradingConditions)
        cleanAllPreviousData('performanceMetrics_market', communityConditions, tradingConditions)
    for communityCondition in communityConditions:
        for tradingCondition in tradingConditions:
            for simroundNo in range(1, 5):
                print(f'{communityCondition}   :::   {tradingCondition}    :::    ROUND {simroundNo}')

                dataDir = f'../testLogs/{communityCondition}/{tradingCondition}/Simround_{simroundNo}'
                communityData = pd.read_csv(os.path.join(dataDir, 'communityLog.csv'))
                allMemberIDs = communityData['memberID'].unique()
                if tradingCondition != 'poolingOnly':
                    communityDataMarket = pd.read_csv(os.path.join(dataDir, 'communityLogWithMarket.csv'))
                timepoints = communityData['timestamp'].unique()
                for timepoint in timepoints:
                    timepointData = communityData[communityData['timestamp'] == timepoint].reset_index()
                    if not by_pool:
                        if tradingCondition != 'marketOnly':
                            calculateAllMetricsPerRound(timepointData, dataDir, False, 'pool', marketPrice)

                        if tradingCondition != 'poolingOnly':
                            timepointDataMarket = communityData[communityDataMarket['timestamp'] == timepoint].reset_index()
                            calculateAllMetricsPerRound(timepointDataMarket, dataDir, False, 'market', marketPrice)
                    else:
                        for poolNo in range(1, 5):
                            poolName = f'pool{poolNo}'
                            calculateMetricsPerRoundForEachPool(timepoint, simroundNo, dataDir, allMemberIDs, False, poolName, marketPrice)

recalculatePerformanceMetrics(by_pool=True)
createCommunityConditionsTables()
createTradingConditionsTables()


            # if (testingMode == 'poolingAndMarket') | (testingMode == 'poolingOnly'):
            #       utilisationRates = pd.read_csv(os.path.join(poolsLogDir, 'acceptanceRates.csv'))
            #
            # print('ROUND {}'.format(simulationRound))
            # print('          Self-sufficiency: {}'.format(communityMetricsPool['Self-sufficiency'].mean()))
            # print('          Self-consumption: {}'.format(communityMetricsPool['Self-consumption'].mean()))