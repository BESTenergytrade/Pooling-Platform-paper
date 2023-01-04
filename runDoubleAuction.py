import os.path

from libraryImports import *
from doubleActionFunctions import *
import numpy as np
import pandas as pd
import pymarket as pm
import matplotlib.pyplot as plt
from pprint import pprint



#load the community log
# communityLogPath = os.path.join(roundLogDir, 'communityLog.csv')
# communityLog = pd.read_csv(communityLogPath)
# timestamps = list(communityLog['timestamp'].unique())

def runDoubleAuctionRound(communityLog, roundLogDir):
    marketParticipants = list(communityLog['memberID'].unique())
    price = np.inf
    transactions = pd.DataFrame()
    while price == np.inf:
        mar = pm.Market()
        #submit the leftover energy as bids and asks
        for lineInd in range(communityLog.shape[0]):
            bidPrice = random.randint(11, 32)
            energy = communityLog.iloc[lineInd]['leftoverEnergy (kwh)']
            participantNo = marketParticipants.index(communityLog.iloc[lineInd]['memberID'])
            if energy < 0:
                buying = True
            else:
                buying = False
            mar.accept_bid(abs(energy), bidPrice, participantNo, buying)

        pm.market.MECHANISM['uniform'] = UniformPrice

        bids = mar.bm.get_df()
        transactions, extras = mar.run('uniform')
        transactions = transactions.get_df()
        if len(transactions) > 0:
            price = transactions['price'][0]
        else:
            price = -111
    if len(transactions) > 0:
        stat = mar.statistics()

        stats = {}
        stats['uniform'] = stat

    transactions.to_csv(os.path.join(roundLogDir, 'market_transactions.csv'))
    #update the community log and save the transactions
    for transactionInd in range(len(transactions)):
        transaction = transactions.iloc[transactionInd]
        memberID = marketParticipants[transaction['bid']]
        if communityLog.loc[communityLog['memberID'] == memberID, 'leftoverEnergy (kwh)'].values[0] >= 0:
            newLeftoverEnergy = communityLog[communityLog['memberID'] == memberID]['leftoverEnergy (kwh)'].values[0] - transaction['quantity']
            if newLeftoverEnergy > communityLog.loc[communityLog['memberID'] == memberID, 'leftoverEnergy (kwh)'].values[0]:
                pass
            communityLog.loc[communityLog['memberID'] == memberID, 'leftoverEnergy (kwh)'] = communityLog.loc[communityLog['memberID'] == memberID, 'leftoverEnergy (kwh)'] - transaction['quantity']

        else:
            communityLog.loc[communityLog['memberID'] == memberID, 'leftoverEnergy (kwh)'] = communityLog.loc[communityLog['memberID'] == memberID, 'leftoverEnergy (kwh)'] + transaction['quantity']


    return communityLog





