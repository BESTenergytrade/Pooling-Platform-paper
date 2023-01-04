import os.path

import matplotlib.pyplot as plt
import pandas as pd
from globalVars import *
from libraryImports import *
from auxFunctions import *


metricsGeneral = {'Self-sufficiency': {'fullMetricName': 'Self-sufficiency', 'shortMetricName': 'SS', 'Colour': '#43DF38', 'ylabel': 'Self-sufficiency (%)'},
           'Self-consumption': {'fullMetricName': 'Self-consumption','shortMetricName': 'SC', 'Colour': '#F71515', 'ylabel': 'Self-consumption (%)'},
           'PCR': {'fullMetricName': 'Production-to-consumption ratio', 'shortMetricName': 'PCR', 'Colour': '#8961C4', 'ylabel': 'Production-to-consumption ratio'},
           'SMS': {'fullMetricName': 'Share of market savings', 'shortMetricName': 'SMS', 'Colour': '#1583E2', 'ylabel': 'Share of market savings (%)'},
           'shareMarketSavingsProd': {'Colour': "#0C53BD"},
            'shareMarketSavingsCons': {'Colour': "#EE4F1E"},
            'acceptanceRate': {'Colour': '#69DFDF', 'ylabel': 'User preference satisfaction (%)'},
            'Consumption': {'Colour': '#FCBA03'},
            'Production': {'Colour': '#039DFC'}
           }

xlabels = {'dayPlot': 'Time step (15mins)'}

dayplotXlocs = [0, 20, 40, 60, 80]

plotFont = {'family': 'Times New Roman', 'size': 16}
poolPlotFont = {'family': 'Times New Roman', 'size': 22}
ticksFont = {'family': 'Times New Roman', 'size': 16}

plotsPath = os.path.join(analysisPath, 'plots')

if not os.path.exists(plotsPath):
    os.makedirs(plotsPath)

def averageOverDays(metricData, metricColumnName):
    metricData['timestamp'] = metricData['timestamp'].str.slice(start=11)

    metricData = metricData.groupby('timestamp', as_index=False).agg({metricColumnName: 'mean'})
    times = metricData['timestamp']
    metric = list(metricData[metricColumnName])
    return times, metric

def getConsumerProsumerData(consprosIDs, communityData, idMapping, dataname):
    memData = {}
    times = []
    consprosIDs = list(consprosIDs)

    if dataname == 'supplyAndDemand':
        colname = 'tradeableEnergy (kwh)'
    else:
        colname = 'acceptanceRate'
        communityData = communityData.rename(columns={'Timestamp': 'timestamp'})

    for consproID in consprosIDs:
        consprosData = communityData[communityData['memberID'] == consproID]
        times, netConsProsData = averageOverDays(consprosData.copy(), colname)
        newID =  idMapping[consproID]
        if dataname == 'supplyAndDemand':
            memData[newID] = netConsProsData
        else:
            memData[newID] = [netConsProsDatum / 20 for netConsProsDatum in netConsProsData]
    return times, memData

def plotOneDayPlot(metricData, label, role, metricColumnName, fig, asSubplots = False, ax=0, subplotNo=0): #role is cons or prod
        plt.figure(fig)
        # discard the date part and only take the time part to group by time
        times, metric = averageOverDays(metricData, metricColumnName)
        colour = 'blue'

        #change colour if it is specified
        if metricColumnName in list(metricsGeneral):
            colour = metricsGeneral[metricColumnName]['Colour']
        elif metricColumnName == 'tradeableEnergy (kwh)':
            if role == 'cons':
                colour = metricsGeneral['Consumption']['Colour']
            elif role == 'prod':
                colour = metricsGeneral['Production']['Colour']

        if asSubplots:
            ax[subplotNo].plot(range(len(times)), metric, label=label, color=colour)
        else:
            plt.plot(range(len(times)), metric, label=label, color=colour)


def plotSupplyAndDemand_dayplot(communityData, communityCondition):
    #get community config from string
    consNo, prosNo = communityCondition.split('_')
    consNo = int(consNo[4:])
    prosNo = int(prosNo[4:])
    memberIDs = communityData['memberID'].unique()
    consumerIDs = memberIDs[:consNo]
    prosumerIDs = memberIDs[consNo:]

    dayPlotDir = os.path.join(plotsPath, 'dayPlots', 'supplyAndDemand')
    if not os.path.exists(dayPlotDir):
        os.makedirs(dayPlotDir)

    #plot the plots
    consFig = plt.figure()
    for consumerID in consumerIDs:
        consumerData = communityData[communityData['memberID'] == consumerID]
        netConsumerData = consumerData.groupby('timestamp', as_index = False).agg({'tradeableEnergy (kwh)': 'sum'})
        plotOneDayPlot(netConsumerData, consumerID, 'cons', 'tradeableEnergy (kwh)', consFig)

    plt.ylabel('Energy (kwh)',  **plotFont)
    plt.xlabel(xlabels['dayPlot'],  **plotFont)
    plt.legend(prop=plotFont)
    plt.savefig(os.path.join(dayPlotDir, f'{communityCondition}_dayPlot_cons.pdf'))

    prosprodFig = plt.figure()
    prosconsFig = plt.figure()
    for prosumerID in prosumerIDs:
        prosumerData = communityData[communityData['memberID'] == prosumerID]
        #prosumerData = prosumerData.groupby('timestamp', as_index = False)

        #extract only production or consumption and make the rest of the timepoints 0 to preserve continuity
        prosConsumption = prosumerData.copy()
        prosConsumption.loc[prosConsumption['tradeableEnergy (kwh)'] >= 0, 'tradeableEnergy (kwh)'] = 0
        plotOneDayPlot(prosConsumption, prosumerID, 'cons',  'tradeableEnergy (kwh)', prosconsFig)
        prosProduction = prosumerData.copy()
        prosProduction.loc[prosProduction['tradeableEnergy (kwh)'] <= 0, 'tradeableEnergy (kwh)'] = 0
        plotOneDayPlot(prosProduction, prosumerID, 'prod', 'tradeableEnergy (kwh)', prosprodFig)



    #save the prosumer consumption plot
    plt.figure(prosprodFig)
    plt.ylabel('Energy (kWh)',  **plotFont)
    plt.xlabel(xlabels['dayPlot'], **plotFont)
    plt.legend(prop=plotFont)
    plt.savefig(os.path.join(dayPlotDir, f'{communityCondition}_dayPlot_pros_prod.pdf'))

    #save the prosumer production plot
    plt.figure(prosconsFig)
    plt.ylabel('Energy (kWh)',  **plotFont)
    plt.xlabel(xlabels['dayPlot'],  **plotFont)
    plt.legend(prop=plotFont)
    plt.savefig(os.path.join(dayPlotDir, f'{communityCondition}_dayPlot_pros_cons.pdf'))

def metricStackplot(communityData, communityCondition, fig, axs, subplotNo, dataName, asSuplot, dayPlotDir):
    #get community config from string
    consNo, prosNo = communityCondition.split('_')
    consNo = int(consNo[4:])
    prosNo = int(prosNo[4:])
    memberIDs = list(communityData['memberID'].unique())
    consumerIDs = list(memberIDs[:consNo])
    prosumerIDs = list(memberIDs[consNo:])

    idMapping = participantIDMapping(memberIDs, communityCondition, 'dict')

    dayPlotDir = os.path.join(plotsPath, 'dayPlots', dataName)
    if not os.path.exists(dayPlotDir):
        os.makedirs(dayPlotDir)

    #plot the plots
    times, consDic = getConsumerProsumerData(consumerIDs, communityData, idMapping, dataName)
    times, prosProdDic = getConsumerProsumerData(prosumerIDs, communityData, idMapping, dataName)
    times, prosConsDic = getConsumerProsumerData(prosumerIDs, communityData, idMapping, dataName)

    allConsumption = consDic | prosConsDic | prosProdDic

    if asSuplot:
        plt.figure(fig)
        axs[subplotNo].stackplot(list(times), allConsumption.values(), labels=allConsumption.keys())

        if dataName == 'supplyAndDemand':
            ylabel = 'Energy (kWh)'
        else:
            ylabel = metricsGeneral['acceptanceRate']['ylabel']

        locs = [0, 20, 40, 60, 80]
        legendProps = plotFont | {'size': 6}
        axs[subplotNo].set_xticklabels(locs)
        axs[subplotNo].set_xticks(locs)
        axs[subplotNo].tick_params(which='major', axis='x', grid_alpha = 0)
        axs[subplotNo].legend(bbox_to_anchor=(0.5, -0.23), ncol = 7, loc='lower center', prop= legendProps )
        axs[subplotNo].set_title(f'Consumers {consNo} - Prosumers {prosNo}', **plotFont)
        axs[subplotNo].set_xlabel(xlabels['dayPlot'], **plotFont)
        axs[subplotNo].set_ylabel(ylabel, **plotFont)
    else:
        fig = plt.figure()
        plt.stackplot(list(times), allConsumption.values(), labels=allConsumption.keys())

        if dataName == 'supplyAndDemand':
            ylabel = 'Energy (kWh)'
            plt.ylim(-25, 43)
        else:
            ylabel = metricsGeneral['acceptanceRate']['ylabel']
            plt.ylim(0, 100)

        locs = [0, 20, 40, 60, 80]
        legendProps = plotFont | {'size': 12}
        plt.xticks(dayplotXlocs, labels=locs, **ticksFont)
        plt.yticks(plt.yticks()[0], **ticksFont)
        plt.tick_params(which='major', axis='x', grid_alpha=0)
        plt.tick_params(which='major', axis='y', grid_alpha=0)
        plt.legend(bbox_to_anchor=(0.5, -0.39), ncol=7, loc='lower center', prop=legendProps)
        plt.title(f'Consumers {consNo} - Prosumers {prosNo}', **plotFont)
        plt.xlabel(xlabels['dayPlot'], **plotFont)
        plt.ylabel(ylabel, **plotFont)
        plt.savefig(os.path.join(dayPlotDir, f'{dataName}_{subplotNo}.pdf'), bbox_inches='tight')


def getAndPlotAsStackplot(metric, tradingCondition, component='', asSubplots=False):
    dayPlotDir = os.path.join(plotsPath, 'dayPlots', metric)

    subplotNo = 0
    supplyDemandFig, axx = plt.subplots(1, 3, sharey=True)
    supplyDemandFig.set_figwidth(16)
    supplyDemandFig.set_figheight(6)

    for communityCondition in communityConditions:
        if metric == 'supplyAndDemand':
            dataAnalysed = pd.read_csv(
                os.path.join(f'../testLogs/{communityCondition}/poolingOnly/Simround_1/', 'communityLog.csv'))
            metricStackplot(dataAnalysed, communityCondition, supplyDemandFig, axx, subplotNo, 'supplyAndDemand', asSubplots, dayPlotDir)


        elif metric == 'preferenceSatisfaction':
            dataAnalysed = pd.DataFrame()
            for simroundNo in range(1, (noSimulationRounds+1)):
                dataAnalysedLocal = pd.read_csv(
                    os.path.join(f'../testLogs/{communityCondition}/{tradingCondition}/Simround_{simroundNo}/', f'acceptanceRates_{component}.csv'))

                dataAnalysed = pd.concat([dataAnalysed, dataAnalysedLocal])

            metricStackplot(dataAnalysed, communityCondition, supplyDemandFig, axx, subplotNo, 'preferenceSatisfaction', asSubplots, dayPlotDir)

        subplotNo += 1

    if asSubplots:
        for ax in axx.flat:
            ax.label_outer()


        plt.savefig(os.path.join(dayPlotDir, f'{metric}_{tradingCondition}_{component}.pdf'))



def plotMetricDayPlots(communityCondition, tradingCondition, simroundNo, component, poolsOnly=False, avg=False):
    rawResultsDir = f'../testLogs/{communityCondition}/{tradingCondition}/Simround_{simroundNo}'

    # retrieve the data
    performanceMetricsFile = os.path.join(rawResultsDir, f'performanceMetrics_{component}.csv')


    if os.path.exists(performanceMetricsFile):
        # if avg:
        #     performanceMetrics = pd.DataFrame()
        #
        #     for simroundNo in range(1, noSimulationRounds+1):
        #         rawResultsDir = f'../testLogs/{communityCondition}/{tradingCondition}/Simround_{simroundNo}'
        #
        #         performanceMetricsFile = os.path.join(rawResultsDir, f'performanceMetrics_{component}.csv')
        #         performanceMetricsLocal = pd.read_csv(performanceMetricsFile)
        #         performanceMetrics = pd.concat([performanceMetrics, performanceMetricsLocal])
        # else:
        #
        #     performanceMetrics = pd.read_csv(performanceMetricsFile)
        #
        # performanceMetrics = performanceMetrics.rename(columns={'Timestamp': 'timestamp'})
        # performanceMetrics['shareMarketSavingsProd'] = performanceMetrics['shareMarketSavingsProd']*(-1)

        dayPlotDir = os.path.join(plotsPath, 'dayPlots')
        if not os.path.exists(dayPlotDir):
            os.makedirs(dayPlotDir)

        if poolsOnly:
            metricsToPlot = ['SMS']
        else:
            metricsToPlot = ['Self-sufficiency', 'Self-consumption', 'SS and SC', 'PCR', 'SMS']

        for metric in metricsToPlot:
            #set and create the saving directory
            if poolsOnly and (metric == 'SMS'):
                dayPlotDirSubdir = os.path.join(dayPlotDir, 'byPool', metric)
            else:
                if avg:
                    dayPlotDirSubdir = os.path.join(dayPlotDir, metric, 'average')
                else:
                    dayPlotDirSubdir = os.path.join(dayPlotDir, metric, 'individual')

            if not os.path.exists(dayPlotDirSubdir):
                os.makedirs(dayPlotDirSubdir)

            plotASpecificDayPlot(metric, communityCondition, tradingCondition, simroundNo, component, outputPlotDirSubdir=dayPlotDirSubdir, avg=avg)
    else:
        print(f'Cannot plot a day plot: file does not exist: {performanceMetricsFile}')

#The no subplots part wasn't finished
def plotPoolMetricPlots(communityCondition, tradingCondition, simroundNo, metric, asSubplots):
    rawResultsDir = f'../testLogs/{communityCondition}/{tradingCondition}/Simround_{simroundNo}'
    dayPlotDir = os.path.join(plotsPath, 'dayPlots')
    dayPlotDirSubdir = os.path.join(dayPlotDir, 'byPool', metric)
    if not os.path.exists(dayPlotDirSubdir):
        os.makedirs(dayPlotDirSubdir)

    if asSubplots:
        fig, ax = plt.subplots(1, 4)
        fig.set_figwidth(16)
        fig.set_figheight(6)
    subplotNo = 0
    for poolNo in range(1, 5):
        currentPool = f'pool{poolNo}'

        # retrieve the data
        performanceMetricsFile = os.path.join(rawResultsDir, f'performanceMetrics_{currentPool}.csv')

        performanceMetrics = pd.read_csv(performanceMetricsFile)
        performanceMetrics = performanceMetrics.rename(columns={'Timestamp': 'timestamp'})
        performanceMetrics['shareMarketSavingsProd'] = performanceMetrics['shareMarketSavingsProd'] * (-1)
        if asSubplots:
            plotOneDayPlot(performanceMetrics.copy(),'Share market savings - production', '', 'shareMarketSavingsProd', fig, ax=ax, asSubplots=True, subplotNo=subplotNo)
            plotOneDayPlot(performanceMetrics.copy(), 'Share market savings - consumption', '', 'shareMarketSavingsCons', fig, ax=ax, asSubplots=True, subplotNo=subplotNo)
            ax[subplotNo].set_title(currentPool, **plotFont)
            ax[subplotNo].set_xlabel(xlabels['dayPlot'], **plotFont)
            ax[subplotNo].set_ylabel(metricsGeneral['SMS']['ylabel'], **plotFont)
        else:
            fig = plt.figure()
            plotOneDayPlot(performanceMetrics.copy(),'Share market savings - production', '', 'shareMarketSavingsProd', fig, asSubplots=False, subplotNo=subplotNo)
            plotOneDayPlot(performanceMetrics.copy(), 'Share market savings - consumption', '', 'shareMarketSavingsCons', fig, asSubplots=False, subplotNo=subplotNo)
            plt.title(currentPool, **poolPlotFont)
            plt.xlabel(xlabels['dayPlot'], **plotFont)
            plt.ylabel(metricsGeneral['SMS']['ylabel'], **plotFont)



        subplotNo += 1

    if asSubplots:
        if metric == 'SMS':
            plt.legend(prop=plotFont, loc='upper right')
        else:
            plt.legend(prop=plotFont)
        # for axx in ax.flat:
        #     axx.set(xlabel=xlabels['dayPlot'], ylabel=metricsGeneral['SMS']['ylabel'], prop=plotFont)

        # Hide x labels and tick labels for top plots and y ticks for right plots.
        for axx in ax.flat:
            axx.label_outer()
        plt.savefig(os.path.join(dayPlotDirSubdir, f'{metricsGeneral[metric]["fullMetricName"]}_{communityCondition}_{tradingCondition}_pools_dayplot_{simroundNo}.pdf'))





#COMPARISON OF CONDITIONS
def plotMetricByCondition(constantConditionName, comparedConditionName, constantData, comparedData, component, metric, asSubplot=False, columnNo=0, metricComponentFig=0, metricComponentAx=0):
    if asSubplot:
        plt.figure(metricComponentFig)
    else:
        metricComponentFig, metricComponentAx = plt.subplots(3, sharex=True)

    componentStage = component

    outputDir = os.path.join(plotsPath, f'Metrics_by_{constantConditionName}')
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    metricComponentFig.set_tight_layout(True)
    subplotNo = 0
    for constantCondition in constantData:
        file = f'{constantCondition}_{comparedConditionName}ConditionStats.csv'
        comparedConditionsStats = pd.read_csv(os.path.join(analysisPath, file))

        # average conditions
        trCondStatsAverage = comparedConditionsStats.agg(['mean'])

        # characteristics of the market
        x_metric_component = []
        y_component = []
        for comparedCondition in comparedData:

            if constantConditionName == 'trading':
                tradingCondition = constantCondition
            else:
                tradingCondition = comparedCondition

            if componentStage == 'Final':
                if tradingCondition == 'poolingOnly':
                    component = 'Pool'
                elif (tradingCondition == 'marketOnly') | (tradingCondition == 'poolingAndMarket'):
                    component = 'Market'


            conditionCol_metric = f'{metricsGeneral[metric]["shortMetricName"]}_{component}_{comparedCondition}'


            metricValue = trCondStatsAverage[conditionCol_metric].values[0]
            if metricValue != -1:
                y_component.append(comparedCondition)
                x_metric_component.append(metricValue)

        if comparedConditionName == 'trading':
            y_component = ['P & M', 'pOnly', 'mOnly']

        #plt.figure(metricComponentFig)
        if constantConditionName == 'community':
            consNo, prosNo = constantCondition.split('_')
            consNo = int(consNo[4:])
            prosNo = int(prosNo[4:])
            title = f'{consNo} Consumers - {prosNo} Prosumers'
        else:
            title = constantCondition
        if asSubplot:
            metricComponentAx[subplotNo, columnNo].set_title(title, **plotFont)
            metricComponentAx[subplotNo, columnNo].set_xlabel(metricsGeneral[metric]['ylabel'], **plotFont)
            metricComponentAx[subplotNo, columnNo].barh(y_component, x_metric_component, color=metricsGeneral[metric]['Colour'])
        else:
            metricComponentAx[subplotNo].set_title(title,  **plotFont)
            metricComponentAx[subplotNo].barh(y_component, x_metric_component, color = metricsGeneral[metric]['Colour'])
            metricComponentAx[subplotNo].set_xlabel(metricsGeneral[metric]['ylabel'])
            metricComponentAx[subplotNo].set_yticklabels(list(y_component), **plotFont)

        #metricComponentAx[subplotNo].errorbar(x_metric_component, y_component, fmt='x', ms=16, color = metricsGeneral[metric]['Colour'])
        # plt.savefig(f'{constantCondition}_Self-sufficiency_Conditions_comparison_market.pdf')

        subplotNo += 1

    if not asSubplot:
        plt.suptitle(metric, **plotFont)
        #plt.ylabel(metric,  **plotFont)
        plt.savefig(os.path.join(outputDir, f'{constantConditionName}_{comparedConditionName}_{metric}_{componentStage}.pdf'), bbox_inches='tight')

def getAndPlotAllConditionCompData(constantConditionName, comparedConditionName):
    constantData, comparedData = '', ''

    if constantConditionName == 'community':
        constantData = communityConditions
        comparedData = tradingConditions
    elif constantConditionName == 'trading':
        constantData = tradingConditions
        comparedData = communityConditions

    components = ['Pool', 'Market', 'Final']

    for component in components:
        plotMetricByCondition(constantConditionName, comparedConditionName, constantData, comparedData, component, 'Self-sufficiency')
        plotMetricByCondition(constantConditionName, comparedConditionName, constantData, comparedData, component, 'Self-consumption')



def changePlot(metricName):
    ssChangeFig, ssChangeAx = plt.subplots(3, sharex=True)

    subplotNo = 0
    y = ['pool', 'pool+market']

    for communityCondition in communityConditions:
        statsData = pd.read_csv(os.path.join(analysisPath, 'poolingAndMarket_communityConditionStats.csv'))

        metricShort = metricsGeneral[metricName]['shortMetricName']
        #plot self-sufficiency
        colnameSS_pool = f'{metricShort}_Pool_{communityCondition}'
        colnameSS_market = f'{metricShort}_Market_{communityCondition}'
        poolSS = statsData[colnameSS_pool].mean()
        marketSS = statsData[colnameSS_market].mean()

        x_ss = [poolSS, marketSS]

        plt.figure(ssChangeFig)
        ssChangeAx[subplotNo].set_title(communityCondition,  **plotFont)
        ssChangeAx[subplotNo].barh(y, x_ss, color=metricsGeneral[metricName]['Colour'])
        #ssChangeAx[subplotNo].errorbar(, y, fmt='x', ms=16, color = metricsGeneral[metricName]['Colour'])


        subplotNo += 1

    plt.figure(ssChangeFig)
    ssChangeFig.tight_layout(pad=0.5)
    plt.xlabel(metricsGeneral[metricName]['ylabel'], **plotFont)
    plt.savefig(os.path.join(plotsPath, f'{metricName}_Change_PoolingAndMarket.pdf'))





from collections import Counter
def plotTransactions(communityCondition, tradingCondition, simroundNo, asSubplots):
    if tradingCondition != 'marketOnly':
        resultsDir = f'../testLogs/{communityCondition}/{tradingCondition}/Simround_{simroundNo}'
        outputDir = os.path.join(plotsPath, 'transactionsPlots')
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)

        communityData = pd.read_csv(os.path.join(resultsDir, 'communityLog.csv'))
        allMemberIds = list(communityData['memberID'].unique())
        idMapping = participantIDMapping(allMemberIds, communityCondition)

        poolsMatrices = {'pool1': [], 'pool2': [], 'pool3': [], 'pool4': []} #rows are members

        for memberID in allMemberIds:
            transFile = f'{memberID}_transactions.csv'
            memberTransactions = pd.read_csv(os.path.join(resultsDir, transFile))

            consumerRows = getMatrixForTransactionsAsRole('consumer', 'producer', memberID, memberTransactions, allMemberIds)
            producerRows = getMatrixForTransactionsAsRole('producer', 'consumer', memberID, memberTransactions, allMemberIds)

            for poolNo in range(1, 5):
                currentPool = f'pool{poolNo}'

                if (len(consumerRows[currentPool]) > 0) & (len(producerRows[currentPool]) > 0):
                    summedRows = [x + y for x, y in zip(consumerRows[currentPool][0], producerRows[currentPool][0])]
                else:
                    summedRows = [0]*len(allMemberIds)

                poolsMatrices[currentPool].append(summedRows)

        fig, axx = plt.subplots(1, 4)
        fig.set_figwidth(16)
        fig.set_figheight(6)
        localFont = plotFont.copy()
        localFont['size'] = 12

        subplotNo = 0
        for poolNo in range(1, 5):
            currentPool = f'pool{poolNo}'


            if asSubplots:
                axx[subplotNo].set_xticks(np.arange(len(allMemberIds)), labels=idMapping,  **localFont)
                axx[subplotNo].set_yticks(np.arange(len(allMemberIds)), labels=idMapping,  **localFont)
                im = axx[subplotNo].imshow(poolsMatrices[currentPool], vmin=0)
                axx[subplotNo].set_title(currentPool, **localFont)

                plt.setp(axx[subplotNo].get_xticklabels(), rotation=45, ha="right",
                         rotation_mode="anchor",  **localFont)
                cbar = plt.colorbar(im, ax=axx[subplotNo], fraction=0.046, pad=0.04)
                cbar.ax.set_ylabel('Energy (kWh)', **localFont)
            else:
                fig, ax = plt.subplots()
                ax.set_xticks(np.arange(len(allMemberIds)), labels=idMapping, **localFont)
                ax.set_yticks(np.arange(len(allMemberIds)), labels=idMapping, **localFont)
                im = plt.imshow(poolsMatrices[currentPool], vmin=0)
                plt.title(currentPool, **poolPlotFont)

                plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
                         rotation_mode="anchor", **localFont)
                cbar = plt.colorbar(im, fraction=0.046, pad=0.04)
                plt.clim(vmin=0)

                cbar.ax.set_ylabel('Energy (kWh)', **localFont)
                plt.savefig(
                    os.path.join(outputDir, f'{communityCondition}_{tradingCondition}_{currentPool}_{simroundNo}.pdf'), bbox_inches='tight')

            subplotNo += 1


        # for ax in axx.flat:
        #     im = ax.imshow(np.random.random((10, 10)), vmin=0, vmax=1)
        if asSubplots:
            fig.tight_layout()
            plt.savefig(os.path.join(outputDir, f'{communityCondition}_{tradingCondition}_allPools_{simroundNo}.pdf'))


def getMatrixForTransactionsAsRole(memberRole, partnerRole, memberID, memberTransactions, allMemberIds):
    transactionsAsRole = memberTransactions[memberTransactions[f'{memberRole}ID'] == memberID]
    memberRows = {'pool1': [], 'pool2': [], 'pool3': [], 'pool4': []}

    transactionsAsRoleTotal = []
    if len(transactionsAsRole) > 0:
        transactionsAsRoleTotal = transactionsAsRole.groupby(['pool', f'{partnerRole}ID']).agg('sum')

    for poolNo in range(1, 4):
        currentPool = f'pool{poolNo}'
        poolMatrixRow = [0] * len(allMemberIds)

        if (len(transactionsAsRoleTotal) > 0) and (currentPool in transactionsAsRoleTotal.index):
            transactionsInPool = transactionsAsRoleTotal.loc[(currentPool,)]

            for rowIndex, row in transactionsInPool.iterrows():
                partnerInd = allMemberIds.index(rowIndex)
                poolMatrixRow[partnerInd] = row['energyTraded (kwh)']

        memberRows[currentPool].append(poolMatrixRow)

    return memberRows

def plotPreferenceSatisfaction(communityCondition, tradingCondition, simroundNo, component, avg=False):

    outputDir = os.path.join(os.path.join(plotsPath, 'userPreferencePlots'))
    if avg:
        dayPlotDir = os.path.join(os.path.join(plotsPath, 'userPreferencePlots', 'dayPlots', 'average'))
    else:
        dayPlotDir = os.path.join(os.path.join(plotsPath, 'userPreferencePlots', 'dayPlots', 'individual'))

    if not os.path.exists(dayPlotDir):
        os.makedirs(dayPlotDir)

    if avg:
        prefSatData = pd.DataFrame()
        for simroundNo in range(1, (noSimulationRounds + 1)):
            dataDir = f'../testLogs/{communityCondition}/{tradingCondition}/Simround_{simroundNo}'
            prefSatDataFromFile = pd.read_csv(os.path.join(dataDir, f'acceptanceRates_{component}.csv'))
            prefSatData = pd.concat([prefSatData, prefSatDataFromFile])
    else:
        dataDir = f'../testLogs/{communityCondition}/{tradingCondition}/Simround_{simroundNo}'
        prefSatData = pd.read_csv(os.path.join(dataDir, f'acceptanceRates_{component}.csv'))


    allMemberIDs = list(prefSatData['memberID'].unique())
    classifiedMemberIDs = participantIDMapping(allMemberIDs, communityCondition)

    prefDataByID = prefSatData.groupby('memberID')


    #get the average for the whole round
    prefSatDataMean = prefDataByID.agg({'acceptanceRate': 'mean'})

    #add data to x_data in the order of memberIDs
    x_data_mean = []
    for memberID in allMemberIDs:
        x_data_mean.append(prefSatDataMean.loc[memberID]['acceptanceRate'])

    x_data = []
    for memberID in allMemberIDs:
        x_data.append(list(prefSatData[prefSatData['memberID'] == memberID]['acceptanceRate']))

    fig = plt.figure()
    plt.errorbar(x_data_mean, classifiedMemberIDs, fmt='x', ms=15, color=metricsGeneral['acceptanceRate']['Colour'])
    plt.xlim(xmin=0)

    plt.xlabel(metricsGeneral['acceptanceRate']['ylabel'],  **plotFont)
    plt.title('User preference satisfaction by user', **plotFont)
    if avg:
        plt.savefig(os.path.join(outputDir, f'{communityCondition}_{tradingCondition}_avg_{component}.pdf'))
    else:
        plt.savefig(os.path.join(outputDir, f'{communityCondition}_{tradingCondition}_round{simroundNo}_{component}.pdf'))

    for memberID, data in prefDataByID:
        data = data.rename(columns = {'Timestamp': 'timestamp'})
        fig = plt.figure()
        plotOneDayPlot(data, memberID, '', 'acceptanceRate', fig)
        plt.xlabel(xlabels['dayPlot'],  **plotFont)
        plt.ylabel(metricsGeneral['acceptanceRate']['ylabel'],  **plotFont)
        plt.savefig(os.path.join(dayPlotDir, f'{memberID}_preferenceSatisfactionDay.pdf'))

def plotASpecificDayPlot(metric, communityCondition, tradingCondition, simroundNo, component, outputPlotDirSubdir ='', asSubplot=False, subplotNo=0, fig=0, ax=0, avg=False):

    if outputPlotDirSubdir == '':
        outputPlotDirSubdir = os.path.join(os.path.join(plotsPath, 'selected'))


    if not os.path.exists(outputPlotDirSubdir):
        os.makedirs(outputPlotDirSubdir)

    if asSubplot:
        plt.figure(fig)
    else:
        # SS and SC together
        fig = plt.figure()

    if avg:
        performanceMetrics = pd.DataFrame()

        for simroundNo in range(1, noSimulationRounds + 1):
            dataDir = f'../testLogs/{communityCondition}/{tradingCondition}/Simround_{simroundNo}'

            performanceMetricsFile = os.path.join(dataDir, f'performanceMetrics_{component}.csv')
            performanceMetricsLocal = pd.read_csv(performanceMetricsFile)
            performanceMetrics = pd.concat([performanceMetrics, performanceMetricsLocal])
    else:
        dataDir = f'../testLogs/{communityCondition}/{tradingCondition}/Simround_{simroundNo}'

        performanceMetrics = pd.read_csv(os.path.join(dataDir, f'performanceMetrics_{component}.csv'))

    performanceMetrics = performanceMetrics.rename(columns={'Timestamp': 'timestamp'})
    performanceMetrics['shareMarketSavingsProd'] = performanceMetrics['shareMarketSavingsProd'] * (-1)


    if (metric == 'SS and SC') | (metric == 'SMS'):
        if metric == 'SS and SC':
            metric1 = colname1 = 'Self-consumption'
            metric2 = colname2 = 'Self-sufficiency'
            ylabel = 'Performance metrics over the day (%)'
        else:
            metric1, colname1 = 'Share of market savings - production', 'shareMarketSavingsProd'
            metric2, colname2 = 'Share of market savings - consumption', 'shareMarketSavingsCons'
            ylabel = metricsGeneral['SMS']['ylabel']


        plotOneDayPlot(performanceMetrics.copy(), metric1, '', colname1, fig, asSubplots=asSubplot, ax=ax, subplotNo=subplotNo)
        plotOneDayPlot(performanceMetrics.copy(), metric2, '', colname2, fig, asSubplots=asSubplot, ax=ax, subplotNo=subplotNo)
        plt.ylabel(ylabel, **plotFont)
        plt.xlabel(xlabels['dayPlot'], **plotFont)

        if (component != 'pool1') & (component != 'pool2') & (component != 'pool3') & (component != 'pool4'):
            plt.title(metricsGeneral[metric]['fullMetricName'])
        else:
            plt.title(component, **poolPlotFont)

        if metric == 'SMS':
            plt.ylim((-100, 100))
        else:
            plt.ylim((0, 100))
        plt.xticks(dayplotXlocs, **ticksFont)
        plt.yticks(plt.yticks()[0], **ticksFont)

        if (metric == 'SMS') & asSubplot & (component != 'pool1') & (component != 'pool2') & (component != 'pool3'):
            plt.legend(prop=plotFont, loc='upper right', bbox_to_anchor=(1.5, 1))
        else:
            plt.legend(prop=plotFont)

        if not asSubplot:
            plt.savefig(os.path.join(outputPlotDirSubdir,
                                 f'{metric}_{communityCondition}_{tradingCondition}_{component}_dayplot_{simroundNo}.pdf'), bbox_inches='tight')

    # the plots with one component
    else:
        plotOneDayPlot(performanceMetrics.copy(), metricsGeneral[metric]['fullMetricName'], '', metric, fig, asSubplots=asSubplot, ax=ax, subplotNo=subplotNo)
        plt.ylabel(metricsGeneral[metric]['ylabel'], **plotFont)
        plt.xlabel(xlabels['dayPlot'], **plotFont)
        plt.xticks(dayplotXlocs, **ticksFont)
        plt.yticks(plt.yticks()[0], **ticksFont)
        plt.ylim((0, 100))
        plt.title(metricsGeneral[metric]['fullMetricName'])

        if not asSubplot:
            plt.savefig(os.path.join(outputPlotDirSubdir,
                                 f'{metricsGeneral[metric]["fullMetricName"]}_{communityCondition}_{tradingCondition}_{component}_dayplot_{simroundNo}.pdf'), bbox_inches='tight')


def plotSelectedSubplots(noOfSubplots, subplotArray, dayPlots=False, comparisonPlots=False):
    outputPlotDirSubdir = os.path.join(os.path.join(plotsPath, 'selected'))
    if not os.path.exists(outputPlotDirSubdir):
        os.makedirs(outputPlotDirSubdir)

    if dayPlots:
        dayplotFig, ax = plt.subplots(1, noOfSubplots)
        dayplotFig.set_figwidth(16)
        dayplotFig.set_figheight(6)

        subplotNo = 0
        for subplot in subplotArray:
            metric, communityCondition, tradingCondition, simroundNo, component = subplot
            if simroundNo == 0:
                averages = True
            else:
                averages = False

            plotASpecificDayPlot(metric, communityCondition, tradingCondition, simroundNo, component, fig=dayplotFig, asSubplot=True, subplotNo=subplotNo, ax=ax, avg=averages)
            ax[subplotNo].set_xlabel(xlabels['dayPlot'], **plotFont)
            ax[subplotNo].set_ylabel(metricsGeneral[metric]['ylabel'], **plotFont)
            ax[subplotNo].set_title(metricsGeneral[metric]['fullMetricName'], **plotFont)
            subplotNo += 1

        plt.savefig(os.path.join(outputPlotDirSubdir, 'subDayplots.pdf'))

    if comparisonPlots:
        #lotMetricByCondition(constantConditionName, comparedConditionName, constantData, comparedData, component, metric, asSubplot=False, columnNo=0, metricComponentFig=0, metricComponentAx=0):
        noOfMetrics = len(subplotArray)
        compplotFig, complotAx = plt.subplots(3, noOfMetrics)
        compplotFig.set_figwidth(16)
        compplotFig.set_figheight(6)

        col = 0
        for subplot in subplotArray:
            metric, constantConditionName, comparedConditionName, component = subplot
            constantData, comparedData = '', ''

            if constantConditionName == 'community':
                constantData = communityConditions
                comparedData = list(tradingConditions)
            elif constantConditionName == 'trading':
                constantData = list(tradingConditions)
                comparedData = communityConditions

            plotMetricByCondition(constantConditionName, comparedConditionName, constantData, comparedData, component, metric, asSubplot=True, columnNo=col, metricComponentFig=compplotFig, metricComponentAx=complotAx)
            col += 1
        plt.savefig(os.path.join(outputPlotDirSubdir, 'comparisonPLots.pdf'))

def plotUserPreferences(tradingCondition, simroundNo):
    dataFile = f'../testLogs/userPreferenceSetups/{tradingCondition}/userPreferences_{simroundNo}.csv'
    outputDir = os.path.join(analysisPath, 'userPreferencePlots')

    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    data = pd.read_csv(dataFile)
    allMemberIds = list(data['memberID'])
    idMapping = participantIDMapping(allMemberIds, communityConditions[1])

    roles = ['Cons', 'Prod']

    for role in roles:
        if role == 'Cons':
            fullRoleName = 'Consumer'
        else:
            fullRoleName = 'Producer'

        consumerPreferences = data.loc[:, data.columns.str.contains(role)].transpose()

        xticks = ['Pool1', 'Pool2', 'Pool3', 'Pool4', 'Market']

        fig, ax = plt.subplots()
        localFont = plotFont.copy()
        localFont['size'] = 12

        ax.set_yticks(np.arange(len(xticks)), labels=xticks, **localFont)
        ax.set_xticks(np.arange(len(allMemberIds)), labels=idMapping, **localFont)
        im = plt.imshow(consumerPreferences, vmin=0, vmax=100)
        plt.title(f'{fullRoleName} Preferences', **plotFont)

        plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
                 rotation_mode="anchor", **localFont)
        im_ratio = consumerPreferences.shape[0] / consumerPreferences.shape[1]
        cbar = plt.colorbar(im, fraction=0.046*im_ratio, pad=0.04)

        cbar.ax.set_ylabel('Energy allocated (%)', **localFont)
        plt.savefig(
            os.path.join(outputDir, f'{fullRoleName}Preferences_{tradingCondition}_{simroundNo}.pdf'), bbox_inches='tight')







def plotSelectedSubplotsAsSeparatePictures(noOfSubplots, subplotArray, dayPlots=False, comparisonPlots=False):
    outputPlotDirSubdir = os.path.join(os.path.join(plotsPath, 'selected'))
    if not os.path.exists(outputPlotDirSubdir):
        os.makedirs(outputPlotDirSubdir)

    subplotNo = 1
    if dayPlots:
        for subplot in subplotArray:
            dayplotFig = plt.figure()

            metric, communityCondition, tradingCondition, simroundNo, component = subplot
            if simroundNo == 0:
                averages = True
            else:
                averages = False

            plotASpecificDayPlot(metric, communityCondition, tradingCondition, simroundNo, component, fig=dayplotFig, asSubplot=False, subplotNo=subplotNo, avg=averages)
            plt.xlabel(xlabels['dayPlot'], **plotFont)
            plt.ylabel(metricsGeneral[metric]['ylabel'], **plotFont)
            plt.title(metricsGeneral[metric]['fullMetricName'], **plotFont)

            plt.savefig(os.path.join(outputPlotDirSubdir, f"{metricsGeneral[metric]['fullMetricName']}_{subplotNo}.pdf"), bbox_inches='tight')

    if comparisonPlots:
        #lotMetricByCondition(constantConditionName, comparedConditionName, constantData, comparedData, component, metric, asSubplot=False, columnNo=0, metricComponentFig=0, metricComponentAx=0):
        noOfMetrics = len(subplotArray)


        col = 0
        for subplot in subplotArray:
            compplotFig = plt.figure()

            metric, constantConditionName, comparedConditionName, component = subplot
            constantData, comparedData = '', ''

            if constantConditionName == 'community':
                constantData = communityConditions
                comparedData = list(tradingConditions)
            elif constantConditionName == 'trading':
                constantData = list(tradingConditions)
                comparedData = communityConditions

            plotMetricByCondition(constantConditionName, comparedConditionName, constantData, comparedData, component, metric, asSubplot=False, columnNo=col, metricComponentFig=compplotFig)
            col += 1

            plt.savefig(os.path.join(outputPlotDirSubdir, f'comparisonPLots_{col}.pdf'))


# getAndPlotAsStackplot('preferenceSatisfaction', 'poolingOnly', 'pool', False)
# getAndPlotAsStackplot('supplyAndDemand', 'poolingAndMarket', 'pool', False)
#
tradingConditionsNames = list(tradingConditions)
subDayPlotsArray = [['Self-sufficiency', communityConditions[1], tradingConditionsNames[1], 0, 'pool'],
            ['Self-consumption', communityConditions[1], tradingConditionsNames[1], 0, 'pool'],
            ['SMS', communityConditions[1], tradingConditionsNames[1], 0, 'pool']]

plotSelectedSubplotsAsSeparatePictures(3, subDayPlotsArray, dayPlots=True)
#
tradingConditionsNames = list(tradingConditions)
subCompPlotsArray = [['Self-sufficiency', 'community', 'trading', 'Final'],
            ['Self-consumption', 'community', 'trading', 'Final']]
#
plotSelectedSubplotsAsSeparatePictures(2, subCompPlotsArray, comparisonPlots=True)
#
# #plotASpecificPlot('Self-sufficiency', 'cons5_pros15', 'poolingAndMarket', 'pool', 2)
# # getAndPlotAllConditionCompData('community', 'trading')
# # getAndPlotAllConditionCompData('trading', 'community')
# # changePlot('Self-sufficiency')
# # changePlot('Self-consumption')
#
#
#
# for tradingCondition in tradingConditions:
#     for simroundNo in range(1, (noSimulationRounds + 1)):
#         plotUserPreferences(tradingCondition, simroundNo)


# for communityCondition in communityConditions:
#     for tradingCondition in tradingConditionsNames:
#         # plotPreferenceSatisfaction(communityCondition, tradingCondition, 0, 'pool', True)
#         for poolNo in range(1, 5):
#             plotMetricDayPlots(communityCondition, tradingCondition, 1, f'pool{poolNo}', poolsOnly=True, avg=True)
#
#         for simroundNo in range(1, (noSimulationRounds+1)):
#             plotTransactions(communityCondition, tradingCondition, simroundNo, asSubplots=False)
#




    #     for simroundNo in range(1, 5):
    #         if tradingCondition != 'marketOnly':
    #             plotMetricDayPlots(communityCondition, tradingCondition, simroundNo, 'pool')
    #             plotPreferenceSatisfaction(communityCondition, tradingCondition, simroundNo, 'pool')
    #             plotPoolMetricPlots(communityCondition, tradingCondition, simroundNo, 'SMS')
    #
    #         if tradingCondition != 'poolingOnly':
    #             plotMetricDayPlots(communityCondition, tradingCondition, simroundNo, 'market')
    #             plotPreferenceSatisfaction(communityCondition, tradingCondition, simroundNo, 'market')



# for ax in axx.flat:
#     ax.set(xlabel=xlabels['dayPlot'], ylabel='Energy (kwh)')

# Hide x labels and tick labels for top plots and y ticks for right plots.




