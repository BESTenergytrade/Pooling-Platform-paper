tradingConditions = {"poolingAndMarket": [True, True], "poolingOnly": [True, False], "marketOnly": [False, True]}
communityConditions = ['cons5_pros15', 'cons10_pros10', 'cons15_pros5']
noSimulationRounds = 4
minsInRound = 60*24*3
noOfTimeslotsInRound = minsInRound // 15

startDate = '2014-09-01T06'
endDate = '2014-09-07'  # not including
dataset = 'dataset2'
debugTradingCondition = ''
debugCommunityCondition = ''

analysisPath = f'../testLogs/Analysis/'