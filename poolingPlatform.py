import asyncio
import os.path

import pandas as pd

from libraryImports import *
from fakeCommunityPlatform import *
from fakeOBSd import *
from fakeBiddingAgent import * #also contains the fake pooling app
#from p2pPoolingAPI import *
#from createCommunity import *
from runDoubleAuction import *
import shutil
from performanceMetrics import *
from globalVars import *

poolingApp = FastAPI()


poolingRunning = 0





class PoolingPlatform:
    poolingRunning = asyncio.Event()
    leftoverEnergyDetermined = asyncio.Event()
    poolsSet = asyncio.Event()

    def __init__(self, memberID):
        self.poolsDictionary = {}  # partition of the pool that calculates energy within the pool
        self.localMemberID = memberID  # the ID of the participant whose instance is running
        self.localLeftoverEnergy = 0
        self.allMatches = []
        self.poolAllocationPercentages = pd.DataFrame()



    # ---------------------------------------------- SET UP PART---------------------------------------------

    # get the tradeable energy from the OBSd, pool configurations from the community platform, and the current market price from the bidding agent
    # to be later used to set up pools
    async def getDataForPoolInitialization(self):
        communityPlatformData = fakeCPDatabase[self.localMemberID].__dict__
        print(communityPlatformData['pools'])
        obsdData = fakeObsdDB[self.localMemberID].__dict__
        biddingAgentData = getMarketPrice()
        print('OwnID: {}'.format(self.localMemberID))

        global simTimestamp
        simTimestamp = obsdData['timeStamp']

        return communityPlatformData, obsdData, biddingAgentData

    # use information from the community platform and the obsd to set up pools
    # uses the market price to set up price policy
    async def initializePools(self, poolConfigurations, greenTradeableEnergy, greyTradeableEnergy, predictedMarketPrice):
        percentNotAllocatedConsumer = 100  # initiate as 100% and subtract
        percentNotAllocatedProducer = 100
        totalTradeableEnergy = greenTradeableEnergy + greyTradeableEnergy

        if dataset == 'debug':
            poolConfigurations['pools'] = [poolConfigurations['pools'][0]]

        # set up pools based on the above info and add them to the pool array
        for poolConfig in poolConfigurations['pools']:
            poolConfig = poolConfig.__dict__

            # -------------------------Calculating energy allocation for this pool------------------------------------
            # to calculate the % of energy that goes directly to the market
            percentNotAllocatedConsumer = percentNotAllocatedConsumer - poolConfig['userEnergyDistributionAsConsumer']
            percentNotAllocatedProducer = percentNotAllocatedProducer - poolConfig['userEnergyDistributionAsProducer']

            self.poolAllocationPercentages[poolConfig['poolID']] = {'consumer': poolConfig['userEnergyDistributionAsConsumer'],
                                                                    'producer': poolConfig['userEnergyDistributionAsProducer']}

            # check if percentNotAllocated >= 0, if not, it's a bug
            userEnergyDistributionAsConsumer = poolConfig['userEnergyDistributionAsConsumer']
            if percentNotAllocatedConsumer < 0:
                userEnergyDistributionAsConsumer = poolConfig[
                                                       'userEnergyDistributionAsConsumer'] + percentNotAllocatedConsumer
                percentNotAllocatedConsumer = 0
                print('total % allocated as consumer exceeds 100%')

            userEnergyDistributionAsProducer = poolConfig['userEnergyDistributionAsProducer']
            if percentNotAllocatedProducer < 0:
                userEnergyDistributionAsProducer = poolConfig[
                                                       'userEnergyDistributionAsProducer'] + percentNotAllocatedProducer
                percentNotAllocatedProducer = 0
                print('total % allocated as producer exceeds 100%')

            # -------------------------Set up an instance of the pool-------------------------


            newLocalPoolMember = localPoolMember(memberID=self.localMemberID,
                                                 poolID=poolConfig["poolID"],
                                                 allowedRole = poolConfig['userRole'],
                                                 greenEnergyPool=poolConfig['userGreenEnergyPreference'],
                                                 userEnergyDistributionAsConsumer=userEnergyDistributionAsConsumer,
                                                 userEnergyDistributionAsProducer=userEnergyDistributionAsProducer,
                                                 totalTradeableEnergy= totalTradeableEnergy,
                                                 greenEnergy=greenTradeableEnergy,
                                                 greyEnergy=greyTradeableEnergy,
                                                 gridLocation=poolConfig["participantGridLocations"][
                                                     poolConfig['participantIDs'].index(self.localMemberID)])

            newPool = Pool(poolID=poolConfig["poolID"],
                           localPoolMember=newLocalPoolMember,
                           members=poolConfig["participantIDs"],
                           energyPrice=0,
                           energyRestrictions=[],
                           gridLocations=poolConfig["participantGridLocations"])

            newPool.energyPrice = newPool.calculatePoolPrice(predictedMarketPrice,
                                                             poolConfig["userPricePolicyOption"],
                                                             poolConfig["userPricePolicyFixedPrice"],
                                                             poolConfig["userPricePolicyPercent"])
            # also set events
            newPool.tradeableEnergyDetermined = asyncio.Event()
            newPool.matchesComputed = asyncio.Event()
            newPool.matchesValidated = asyncio.Event()

            # add the pool to the pools dictionary
            self.poolsDictionary[newPool.poolID] = newPool
        self.poolsSet.set()

        # if there is energy that is not allocated to any pool, add to leftover energy
        if totalTradeableEnergy >= 0:
            self.localLeftoverEnergy = totalTradeableEnergy * percentNotAllocatedProducer / 100
        else:
            self.localLeftoverEnergy = totalTradeableEnergy * percentNotAllocatedConsumer / 100





    # ---------------------------------------------- RUNNING PART---------------------------------------------

    # set up and run all pools concurrently in an asynchronous manner
    # send the results to the bidding agent
    async def executePoolingPlatformRound(self):
        print('starting to execute the PP round')

        # use the information from the community platform, obsd, and the bidding agent to set up pools
        communityPlatformData, obsdData, biddingAgentData = await self.getDataForPoolInitialization()
        await self.initializePools(communityPlatformData, obsdData['greenEnergyKwh'], obsdData['greyEnergyKwh'],
                                   biddingAgentData['marketPriceEurocentPerKwh'])

        energyAllocatedToPools = obsdData['greenEnergyKwh'] + obsdData['greyEnergyKwh'] - self.localLeftoverEnergy #for the records

        # get pool coroutines to be able to run them concurrently
        poolCoroutines = []
        for pool in self.poolsDictionary:
            poolCoroutines.append(self.poolsDictionary[pool].executePoolRound())

        # get pool coroutines to be able to run them concurrently
        poolingResults = await asyncio.gather(*poolCoroutines)
        for poolResult in poolingResults:
            self.localLeftoverEnergy += poolResult[0]
            self.allMatches.extend(poolResult[1])

        totalTradeableEnergy = obsdData['greenEnergyKwh'] + obsdData['greyEnergyKwh']
        outputDF = self.writeEnergyLog(totalTradeableEnergy, energyAllocatedToPools)
        print('Leftover evergy: {}'.format(self.localLeftoverEnergy))
        self.leftoverEnergyDetermined.set()
        return outputDF

#-------------------------------------------LOGGING-------------------------------------------------------------------

    def writeEnergyLog(self, initialTotalTradeableEnergy, energyInPools):
        # open or create a file
        if os.path.exists(communityLog):
            file = pd.read_csv(communityLog, index_col=False)
        else:
            file = pd.DataFrame()

        tmp = pd.DataFrame({'memberID': [self.localMemberID],
                            'tradeableEnergy (kwh)': [initialTotalTradeableEnergy],
                            'energyAllocatedToPools (kwh)': [energyInPools],
                            'leftoverEnergy (kwh)': [self.localLeftoverEnergy],
                            'timestamp': [simTimestamp],
                            'simRound': [simulationRound]})

        file = pd.concat([file, tmp], ignore_index=True)
        file.to_csv(communityLog, index=False)
        return tmp


class Pool:
    gridFees = [[0, 1, 2, 3],
                [1, 0, 1, 2],
                [2, 1, 0, 1],
                [3, 2, 1, 0]]

    def __init__(self, poolID, localPoolMember, members, energyPrice, energyRestrictions, gridLocations):
        # owner info
        self.localPoolMember = localPoolMember
        self.energyInPoolBeforeMatching = 0

        # pool info
        self.poolID = poolID
        self.memberIDs = members  # the IDs of the members that would be used to retrieve the information
        self.memberObjects = []  # all participants objects would go here
        self.energySellers = {}
        self.energyBuyers = {}
        self.notParticipating = {}
        self.energyPrice = energyPrice  # price per kw in the pool, to be set when pool is set ip
        self.energyRestrictions = energyRestrictions  # not being developed in early versions
        self.memberGridLocations = gridLocations  # TODO: can be optimised

        # events
        self.tradeableEnergyDetermined = ...
        self.matchesComputed = ...
        self.matchesValidated = ...

        # logging
        self.logFileName = os.path.join(poolsLogDir, '{}_transactions.csv'.format(self.localPoolMember.memberID))

    # use the market price information and price policy to determine the price of energy in the pool
    def calculatePoolPrice(self, marketPrice, pricePolicy, fixedPrice, percentPrice):
        if pricePolicy == 0:  # just using the current price
            return marketPrice
        elif pricePolicy == 1:  # fixed price that's not the market price
            return fixedPrice
        elif pricePolicy == 2:
            return marketPrice * percentPrice / 100

    # run this pool for one round, output the remaining energy and all matches to the pooling platform
    async def executePoolRound(self):
        print('{}: pooling round started'.format(self.poolID))

        if not os.path.exists(poolsLogDir):
            os.mkdir(poolsLogDir)

        self.localPoolMember.determineTradeableEnergyAndRole()
        self.energyInPoolBeforeMatching = self.localPoolMember.tradeableEnergyInPool
        self.tradeableEnergyDetermined.set()
        await self.sendEnergyInfoToAllOtherPoolMembers()
        poolMembers = await self.getEnergyInfoFromAllOtherPoolMembers()
        self.sortPoolMembers(poolMembers)
        memberInfoString = self.createMemberInfoString()
        print(memberInfoString)

        matches = self.matchByGridFeeImproved()
        self.matchesComputed.set()

        await self.sendMatchesForValidation(matches)
        validatedMatches = await self.validateAllMatches(matches)
        self.matchesValidated.set()
        matchesStr = self.createMatchesString(validatedMatches)
        print(matchesStr)
        self.recordTransactionsInTheLog(validatedMatches)

        leftoverEnergy = self.energyInPoolBeforeMatching + self.calculateEnergyChangeFromValidatedMatches(
            validatedMatches)
        return leftoverEnergy, validatedMatches

    # get trading info from all members of the pool for the later matching
    async def getEnergyInfoFromAllOtherPoolMembers(self):  # async version
        # collect the list of all functions that get trading data of pool members
        coroutines = []
        for memberID in self.memberIDs:
            if memberID != self.localPoolMember.memberID:
                coroutines.append(self.getEnergyInfoFromOneOtherPoolMember(memberID))

        # run them concurrently
        members = await asyncio.gather(*coroutines)
        print("{}: Other pool members' trading info retrieved".format(self.poolID, self.localPoolMember.memberID))
        return members

    # get trading info from a member to use it for the matching
    async def getEnergyInfoFromOneOtherPoolMember(self, otherMemberID):
        async with AsyncClient(app=poolingApp, base_url=f"http://{baseIP}/") as client:
            memberInfo = await client.get(f"http://{baseIP}/interPoolCommunication/{otherMemberID}/tradeableEnergy/{self.poolID}/{self.localPoolMember.memberID}")
        memberInfo = json.loads(memberInfo.json()['payload'])  # info for that member for this pool
        if memberInfo['role'] == -11:
            #print('{}: request error => retry'.format(otherMemberID))
            await asyncio.sleep(2)
            async with AsyncClient(app=poolingApp, base_url=f"http://{baseIP}/") as client:
                memberInfo = await client.get(
                    f"http://{baseIP}/interPoolCommunication/{otherMemberID}/tradeableEnergy/{self.poolID}/{self.localPoolMember.memberID}")
            memberInfo = json.loads(memberInfo.json()['payload'])  # info for that member for this pool
        member_index = self.memberIDs.index(otherMemberID)
        otherMemberID = poolMember(memberID=memberInfo["memberID"],
                                   roleInRound = memberInfo['role'],
                                   tradeableEnergyInPool=memberInfo["tradeableEnergyInPool"],
                                   gridLocation=self.memberGridLocations[member_index])
        return otherMemberID

    async def sendEnergyInfoToAllOtherPoolMembers(self):
        coroutines = []
        for memberID in self.memberIDs:
            if memberID != self.localPoolMember.memberID:
                coroutines.append(self.sendEnergyInfoToOneOtherPoolMember(memberID))
        await asyncio.gather(*coroutines)

    async def sendEnergyInfoToOneOtherPoolMember(self, targetMemberID):
        tradeableEnergy = self.energyInPoolBeforeMatching
        memberRole = self.localPoolMember.roleInRound

        message = {"poolID": self.poolID, "memberID": self.localPoolMember.memberID, "role": memberRole,
                   "tradeableEnergyInPool": tradeableEnergy}
        message = createInterpoolMessage(self.localPoolMember.memberID, targetMemberID, 'tradeableEnergy', json.dumps(message))

        async with AsyncClient(app=poolingApp, base_url=f"http://{baseIP}/") as client:
            result = await client.post(f'/interPoolCommunication/{self.localPoolMember.memberID}/tradeableEnergy/{self.poolID}/{targetMemberID}', json=message)


    # sort pool members into sellers and buyers
    def sortPoolMembers(self, poolMembers):
        poolMembers.append(self.localPoolMember)
        for member in poolMembers:
            if member.roleInRound == 1:  # if the person is a potential seller
                self.energySellers[member.memberID] = member  # all pool members
            elif member.roleInRound == 0:  # if the person is a potential buyer
                self.energyBuyers[member.memberID] = member
            else: #no tradeable energy and error codes
                self.notParticipating[member.memberID] = member
        # sort sellers and buyers by member ID
        # self.energySellers.sort(key=lambda x: x.memberID, reverse=True)
        # self.energyBuyers.sort(key=lambda x: x.memberID, reverse=True)

    # match sellers and buyers based on the respective grid fees between them
    # record the matchs and save them in the respective pool member objects
    def matchByGridFee(self):
        localPoolMembersMatches = []
        if self.localPoolMember.roleInRound != -1:
            for buyer in self.energyBuyers:
                # determine all pairwise grid fees for this seller and sort the buyers by them
                sellerGridFees = pd.DataFrame(columns=['memberID', 'gridFee'])
                if len(self.energySellers) > 0:
                    for seller in self.energySellers:
                        sellerGridFees = pd.concat([sellerGridFees, pd.DataFrame(
                            [[seller.memberID, self.gridFees[seller.gridLocation][buyer.gridLocation]]],
                            columns=['memberID', 'gridFee'])])
                    # sort the buyers by the grid fee and get the order of indexes
                    sellerGridFees = sellerGridFees.reset_index(drop=True)
                    sellerGridFees = sellerGridFees.sort_values(by=['gridFee'])
                    sellerOrder = list(
                        sellerGridFees.index)  # list of indices where the first is the index of the buyer with the lowest grid fee etc

                    # go through the buyers sorted by grid fee and distribute energy based on that
                    for sellerID in sellerOrder:
                        seller = self.energySellers[sellerID]

                        if (buyer.tradeableEnergyInPool != 0) & (seller.tradeableEnergyInPool != 0):
                            seller.tradeableEnergyInPool, buyer.tradeableEnergyInPool, match = self.executeMatch(
                                seller, buyer)
                        else:
                            # add empty match to the record
                            match = poolMatch(seller.memberID, buyer.memberID, 0, 0, 0)
                        if (seller.memberID == self.localPoolMember.memberID) | (
                                buyer.memberID == self.localPoolMember.memberID):
                            localPoolMembersMatches.append(match)
                        seller.currentMatches.append(match)
                        buyer.currentMatches.append(match)

            # also record null matchs for the members not participating in this trading round
            for member in self.notParticipating:
                if self.localPoolMember.roleInRound == 1:
                    match = poolMatch(self.localPoolMember.memberID, member.memberID, 0, 0, 0)
                else:
                    match = poolMatch(member.memberID, self.localPoolMember.memberID, 0, 0, 0)

                member.currentMatches.append(match)
                self.localPoolMember.currentMatches.append(match)
                localPoolMembersMatches.append(match)

        else:
            for seller in self.energySellers:
                match = poolMatch(self.localPoolMember.memberID, seller.memberID, 0, 0, 0)
                self.localPoolMember.currentMatches.append(match)
                localPoolMembersMatches.append(match)
            for buyer in self.energyBuyers:
                match = poolMatch(self.localPoolMember.memberID, buyer.memberID, 0, 0, 0)
                self.localPoolMember.currentMatches.append(match)
                localPoolMembersMatches.append(match)

        self.matchesToDict()
        return localPoolMembersMatches

    def matchByGridFeeImproved(self):
        localPoolMembersMatches = []

        if self.localPoolMember.roleInRound != -1:
            buyerSellerGridFees = self.makeSellerBuyerTable()

            #match all lowest grid fees
            gridFeesOrdered = list(buyerSellerGridFees['gridFee'].unique())
            for gridFee in gridFeesOrdered:
                transactionsWithThisGridFee = buyerSellerGridFees[buyerSellerGridFees['gridFee'] == gridFee]

                #if there are any cases where supply and demand are equal, execute them first
                firstPriority = transactionsWithThisGridFee[
                    abs(transactionsWithThisGridFee['buyerTradeableEnergy']) == transactionsWithThisGridFee[
                        'sellerTradeableEnergy']]

                for index, row in firstPriority.iterrows():
                    localPoolMembersMatches = self.executeDFMatchRow(row,
                                                                     localPoolMembersMatches)

                #also update the tables
                buyerSellerGridFees = self.makeSellerBuyerTable()
                transactionsWithThisGridFee = buyerSellerGridFees[buyerSellerGridFees['gridFee'] == gridFee]
                transactionsWithThisGridFee = transactionsWithThisGridFee.sort_values(by=['sellerTradeableEnergy'], ascending=False)

                #then loop through sellers and distribute the remnants
                sellerIDs = transactionsWithThisGridFee['sellerID'].unique()
                for sellerID in sellerIDs:
                    sellersTransactions = transactionsWithThisGridFee[transactionsWithThisGridFee['sellerID'] == sellerID]

                    # if there is only one buyer for the seller, just execute the match
                    if len(sellersTransactions) == 1:
                        localPoolMembersMatches = self.executeDFMatchRow(sellersTransactions, localPoolMembersMatches)

                    elif len(sellersTransactions) > 1:
                        # first match the ones where the supply and demand are equal

                        secondPriority = sellersTransactions[
                            abs(sellersTransactions['buyerTradeableEnergy']) != sellersTransactions[
                                'sellerTradeableEnergy']]

                        if len(secondPriority) > 0:
                            localPoolMembersMatches = self.distributeEnergyByWeight(secondPriority,
                                                                                    localPoolMembersMatches)
                            buyerSellerGridFees = self.updateBuyersTradeableEnergy(secondPriority, buyerSellerGridFees)
                            transactionsWithThisGridFee = self.updateBuyersTradeableEnergy(secondPriority, transactionsWithThisGridFee)


            # also record null matchs for the members not participating in this trading round
            for memberID in self.notParticipating:
                member = self.notParticipating[memberID]
                if self.localPoolMember.roleInRound == 1:
                    match = poolMatch(self.localPoolMember.memberID, member.memberID, 0, 0, 0)
                else:
                    match = poolMatch(member.memberID, self.localPoolMember.memberID, 0, 0, 0)

                member.currentMatches.append(match)
                self.localPoolMember.currentMatches.append(match)
                localPoolMembersMatches.append(match)

        else:
            for sellerID in self.energySellers:
                seller = self.energySellers[sellerID]
                match = poolMatch(self.localPoolMember.memberID, seller.memberID, 0, 0, 0)
                self.localPoolMember.currentMatches.append(match)
                localPoolMembersMatches.append(match)
            for buyerID in self.energyBuyers:
                buyer = self.energyBuyers[buyerID]
                match = poolMatch(self.localPoolMember.memberID, buyer.memberID, 0, 0, 0)
                self.localPoolMember.currentMatches.append(match)
                localPoolMembersMatches.append(match)

        self.matchesToDict()
        return localPoolMembersMatches

    def makeSellerBuyerTable(self):
        # determine all pairwise grid fees for this seller and sort the buyers by them
        buyerSellerGridFees = pd.DataFrame(
            columns=['buyerID', 'sellerID', 'gridFee', 'buyerTradeableEnergy', 'sellerTradeableEnergy'])

        # compile the table of grid fees for buyers and sellers
        for buyerID in self.energyBuyers:
            buyer = self.energyBuyers[buyerID]
            if len(self.energySellers) > 0:
                for sellerID in self.energySellers:
                    seller = self.energySellers[sellerID]
                    buyerSellerGridFees = pd.concat([buyerSellerGridFees, pd.DataFrame(
                        [[buyer.memberID, seller.memberID, self.gridFees[seller.gridLocation][buyer.gridLocation],
                          buyer.tradeableEnergyInPool, seller.tradeableEnergyInPool]],
                        columns=['buyerID', 'sellerID', 'gridFee', 'buyerTradeableEnergy', 'sellerTradeableEnergy'])])


        # sort the buyers by the grid fee and get the order of indexes
        buyerSellerGridFees = buyerSellerGridFees[(buyerSellerGridFees['sellerTradeableEnergy'] > 0) & (buyerSellerGridFees['buyerTradeableEnergy'] < 0)]
        buyerSellerGridFees = buyerSellerGridFees.sort_values(by=['gridFee'])
        buyerSellerGridFees = buyerSellerGridFees.reset_index(drop=True)


        return buyerSellerGridFees

    #update the tradeable energy for the buyers in the table after transactions are completed
    def updateBuyersTradeableEnergy(self, transactionsTable, buyerSellerGridFees):
        for index, transaction in transactionsTable.iterrows():
            buyerSellerGridFees.loc[buyerSellerGridFees['buyerID'] == transaction['buyerID'], 'buyerTradeableEnergy'] = self.energyBuyers[transaction['buyerID']].tradeableEnergyInPool
        return buyerSellerGridFees

    # format the transaction history as a dict
    def matchesToDict(self):
        matchesDict = {}
        for match in self.localPoolMember.currentMatches:
            if self.localPoolMember.roleInRound == 0:
                matchesDict[match.producerID] = match
            else:
                matchesDict[match.consumerID] = match
        self.localPoolMember.currentMatches = matchesDict

    def executeDFMatchRow(self, dfRow, localPoolMembersMatches):
        if dfRow.index[0] == 'buyerID':
            sellerID = dfRow.loc['sellerID']
            buyerID = dfRow.loc['buyerID']
        else:
            index = dfRow.index[0]
            sellerID = dfRow.loc[index, 'sellerID']
            buyerID = dfRow.loc[index, 'buyerID']


        seller = self.energySellers[sellerID]

        buyer = self.energyBuyers[buyerID]

        if (buyer.tradeableEnergyInPool != 0) & (seller.tradeableEnergyInPool != 0):
            seller.tradeableEnergyInPool, buyer.tradeableEnergyInPool, match = self.executeMatch(
                seller, buyer)
        else:
            # add empty match to the record
            match = poolMatch(seller.memberID, buyer.memberID, 0, 0, 0)

        if (seller.memberID == self.localPoolMember.memberID) | (
                buyer.memberID == self.localPoolMember.memberID):
            localPoolMembersMatches.append(match)
        seller.currentMatches.append(match)
        buyer.currentMatches.append(match)

        return localPoolMembersMatches



    def distributeEnergyByWeight(self, dfRows, localPoolMembersMatches):
        sellerID = dfRows['sellerID'].values[0]
        seller = self.energySellers[sellerID]
        sellerOriginalTradeableEnergy = dfRows.loc[dfRows['sellerID'] == sellerID, 'sellerTradeableEnergy'].values[0]

        buyerIDs = dfRows['buyerID'].unique()

        allBuyersDemand = dfRows['buyerTradeableEnergy'].sum()

        if allBuyersDemand < 0:
            for buyerID in buyerIDs:
                buyer = self.energyBuyers[buyerID]
                buyersTradeableEnergy = dfRows.loc[dfRows['buyerID'] == buyerID, 'buyerTradeableEnergy'].values[0]

                allocatedEnergy = sellerOriginalTradeableEnergy * (buyersTradeableEnergy / allBuyersDemand)


                sellersTradeableEnergy, buyer.tradeableEnergyInPool, match = self.executeMatch(seller, buyer, sellersTradeableEnergy=allocatedEnergy, buyersTradeableEnergy=buyer.tradeableEnergyInPool)

                seller.tradeableEnergyInPool -= match.energyTraded
                if abs(seller.tradeableEnergyInPool) < 0.000001:
                    seller.tradeableEnergyInPool = 0

                if (seller.memberID == self.localPoolMember.memberID) | (
                        buyer.memberID == self.localPoolMember.memberID):
                    localPoolMembersMatches.append(match)
                seller.currentMatches.append(match)
                buyer.currentMatches.append(match)

        return localPoolMembersMatches



    # conduct one match and calculate how much energy the seller and the buyer have left after it
    # save a record of the match in the seller and the buyer member objects
    def executeMatch(self, seller, buyer, sellersTradeableEnergy=None, buyersTradeableEnergy=None):
        if (sellersTradeableEnergy == None) & (buyersTradeableEnergy == None):
            # keep the match in the pool and member records
            sellersTradeableEnergy, buyersTradeableEnergy = seller.tradeableEnergyInPool, buyer.tradeableEnergyInPool

        # energy after the match
        # if positive - it's the remaining energy of the seller
        # if negative - of the buyer
        appliedMatchedEnergy = sellersTradeableEnergy + buyersTradeableEnergy

        # update tradeable energy for the owner and partner
        if appliedMatchedEnergy > 0:  # if the seller fully satisfied buyer's needs
            record = poolMatch(seller.memberID, buyer.memberID, abs(buyersTradeableEnergy),
                               self.energyPrice,
                               gridUsageFee=self.gridFees[seller.gridLocation][buyer.gridLocation])
            buyersTradeableEnergy = 0
            sellersTradeableEnergy = appliedMatchedEnergy

        else:  # if the buyer's needs are not fully satisfied
            record = poolMatch(seller.memberID, buyer.memberID, abs(sellersTradeableEnergy),
                               self.energyPrice,
                               gridUsageFee=self.gridFees[seller.gridLocation][buyer.gridLocation])
            sellersTradeableEnergy = 0
            buyersTradeableEnergy = appliedMatchedEnergy

        return sellersTradeableEnergy, buyersTradeableEnergy, record

    async def sendMatchesForValidation(self, matches):
        coroutines = []

        for match in matches:
                coroutines.append(self.sendOneMatchForValidation(match))
        await asyncio.gather(*coroutines)

    async def sendOneMatchForValidation(self, matchWithTheMember):
        if self.localPoolMember.roleInRound == 0:
            targetMemberID = matchWithTheMember.producerID
        else:
            targetMemberID = matchWithTheMember.consumerID

        matchMessage = json.dumps(matchWithTheMember.createMessage())
        message = createInterpoolMessage(self.localPoolMember.memberID, targetMemberID, 'matches', matchMessage)

        async with AsyncClient(app=poolingApp, base_url=f"http://{baseIP}/") as client:
            await client.post(f'/interPoolCommunication/{self.localPoolMember.memberID}/matches/{self.poolID}/{targetMemberID}', json=message)

    # loop through matchs and validate them against what the partners got
    # output a list of validated matchs
    async def validateAllMatches(self, matches):
        coroutines = []
        for match in matches:
            coroutines.append(self.validateOneMatch(match))

        validatedMatches = await asyncio.gather(*coroutines)
        validatedMatches = list(filter(None, validatedMatches))

        print('Pool {}: Matches validated'.format(self.poolID))
        return validatedMatches

    # take one match, get the equivalent match from the trading partner and compare
    # output the match if they match and None if they don't
    async def validateOneMatch(self, match):
        if self.localPoolMember.roleInRound == 0:
            otherMemberID = match.producerID
        else:
            otherMemberID = match.consumerID

        async with AsyncClient(app=poolingApp, base_url=f"http://{baseIP}/") as client:
            otherMembersMatch = await client.get(f"http://{baseIP}/interPoolCommunication/%s/matches/%s/%s" % (
                otherMemberID,
                self.poolID,
                self.localPoolMember.memberID))
        otherMembersMatch = json.loads(otherMembersMatch.json()['payload'])
        if otherMembersMatch['energyKwh'] == 'none': #if the get request was unsuccessful => resend
            #print('Match retrieval problem => resend request')
            await asyncio.sleep(0.2)
            async with AsyncClient(app=poolingApp, base_url=f"http://{baseIP}/") as client:
                otherMembersMatch = await client.get(f"http://{baseIP}/interPoolCommunication/%s/matches/%s/%s" % (
                    otherMemberID,
                    self.poolID,
                    self.localPoolMember.memberID))
            otherMembersMatch = json.loads(otherMembersMatch.json()['payload'])

        # check if the match is the same
        # if not, add this amount back to the owner's tradeable energy
        if otherMembersMatch['energyKwh'] == match.energyTraded:
            return otherMembersMatch
        else:
            # just return an empty match
            return None

    # apply the validated matchs to the energy of the local pool user, output remaining
    def calculateEnergyChangeFromValidatedMatches(self, validatedMatches):
        clearedEnergyChange = 0
        for match in validatedMatches:
            if self.localPoolMember.roleInRound == 0:  # if buyer
                clearedEnergyChange += match['energyKwh']
            elif self.localPoolMember.roleInRound == 1:  # if seller
                clearedEnergyChange -= match['energyKwh']
        # return the final tradeable (leftover) energy of the owner to be later sent to the bidding agent
        return clearedEnergyChange

    #---------------------------------------FOR NEW LOGGING------------------------------------------------------

    def recordTransactionsInTheLog(self, matches):
        outputColumns = ['producerID', 'consumerID', "energyTraded (kwh)", 'rate (ct/kwh)', 'gridUsageFee (ct/kwh)',
                         'totalPrice (ct)', 'pool', 'allocatedEnergy (kwh)', 'timestamp', 'simRound']

        #open or create a file
        if os.path.exists(self.logFileName):
            file = pd.read_csv(self.logFileName, index_col=False)
        else:
            file = pd.DataFrame(columns=outputColumns)

        matchColumns = ['producerID', 'consumerID', 'energyKwh', 'energyPoolPrice', 'gridUsageFee']

        for match in matches:
            if match['energyKwh'] > 0:
                match_df = pd.DataFrame(match, columns=matchColumns, index=[0])
                match_df = match_df.rename(columns={'energyPoolPrice': 'rate (ct/kwh)',
                                                    'gridUsageFee': 'gridUsageFee (ct/kwh)',
                                                    'energyKwh': "energyTraded (kwh)"})
                if self.localPoolMember.roleInRound == 0: #if consumer
                    match_df['totalPrice (ct)'] = (match['energyPoolPrice'] + match['gridUsageFee'])*match['energyKwh']
                else:
                    match_df['totalPrice (ct)'] = match['energyPoolPrice'] * match['energyKwh']
                match_df['pool'] = self.poolID
                match_df['allocatedEnergy (kwh)'] = self.energyInPoolBeforeMatching
                match_df['timestamp'] = simTimestamp
                match_df['simRound'] = simulationRound
                file = pd.concat([file, match_df], ignore_index=True)
        file.to_csv(self.logFileName, index=False)

    #---------------------------------------FOR OLD LOGGING------------------------------------------------------
    def createMemberGroupString(self, membersRole, membersGroup):
        membersString = '{} \n'.format(membersRole)
        for memberID in membersGroup:
            member = membersGroup[memberID]
            mString = 'MemberID: {} | Tradeable Energy: {}\n'.format(member.memberID, member.tradeableEnergyInPool)
            membersString += mString
        membersString += '\n'
        return membersString

    def createMemberInfoString(self):
        memberInfoString = '\nPOOL MEMBERS \n'
        memberInfoString += self.createMemberGroupString('Energy Sellers', self.energySellers)
        memberInfoString += self.createMemberGroupString('Energy Buyers', self.energyBuyers)
        memberInfoString += self.createMemberGroupString('Not participating in the round', self.notParticipating)
        return memberInfoString

    def createMatchesString(self, matches):
        transactions = '\nTRANSACTIONS WITH THE LOCAL MEMBER \n'
        for match in matches:
            matchstring = 'Producer: {} | Consumer: {} | Energy traded: {} | Grid fee: {} \n'.format(
                match['producerID'], match['consumerID'], match['energyKwh'], match['gridUsageFee'])
            transactions += matchstring
        return transactions


# basic information container for each pool member
class poolMember:
    def __init__(self, memberID, roleInRound, tradeableEnergyInPool, gridLocation):
        self.memberID = memberID
        self.roleInRound = roleInRound # 1 - seller, 0 - buyer, -1 - undefined, undefined by default
        self.tradeableEnergyInPool = tradeableEnergyInPool  # received from the members' respective PP instances
        self.clearedTradeableEnergy = tradeableEnergyInPool  # starts with this and then cleared matchs are applied
        self.gridLocation = gridLocation  # in case it's needed for the grid matrix
        self.currentMatches = []  # history of matchs involving this member


# the pool member whose pooling platform instance this is
class localPoolMember(poolMember):
    def __init__(self, memberID, poolID, greenEnergyPool, allowedRole, userEnergyDistributionAsProducer, userEnergyDistributionAsConsumer,
                 totalTradeableEnergy, greenEnergy, greyEnergy, gridLocation):
        poolMember.__init__(self, memberID=memberID, roleInRound=-1, tradeableEnergyInPool=0, gridLocation=gridLocation)
        self.poolID = poolID
        self.greenEnergyPool = greenEnergyPool
        self.allowedRole = allowedRole #the role that the person joined the pool as
        self.userEnergyDistributionAsProducer = userEnergyDistributionAsProducer
        self.userEnergyDistributionAsConsumer = userEnergyDistributionAsConsumer
        self.totalTradeableEnergy = totalTradeableEnergy
        self.greenEnergy = greenEnergy
        self.greyEnergy = greyEnergy

    # determine tradeable energy allocated to this pool and whether the person is a buyer or a seller in this round
    def determineTradeableEnergyAndRole(self):
        if self.memberID == 'H19':
            pass
        # determine owner's role
        if (self.totalTradeableEnergy > 0) & (self.allowedRole > 0):  # which percentage to multiply by depends on it
            self.tradeableEnergyInPool = self.totalTradeableEnergy * self.userEnergyDistributionAsProducer / 100

            #if it's a green energy pool and there isn't enough green energy, just take what is there
            if (self.tradeableEnergyInPool > self.greenEnergy) & (self.greenEnergyPool == 1):
                self.tradeableEnergyInPool = self.greenEnergy

            self.roleInRound = 1  # seller
        elif (self.totalTradeableEnergy < 0) & ((self.allowedRole == 0) | (self.allowedRole == 2)):
            self.tradeableEnergyInPool = self.totalTradeableEnergy * self.userEnergyDistributionAsConsumer / 100
            self.roleInRound = 0  # buyer

        if self.tradeableEnergyInPool == -0.0:
            self.roleInRound = -1

        print('Pool %s: own tradeable energy determined' % (self.poolID))
        print('Pool %s: energy: %.2f' % (self.poolID, self.tradeableEnergyInPool))


# a record for an individual match
class poolMatch:
    def __init__(self, producerID, consumerID, energyTraded, energyPrice, gridUsageFee):
        self.producerID = producerID
        self.consumerID = consumerID
        self.energyTraded = energyTraded
        self.energyPoolPrice = energyPrice
        self.gridUsageFee = gridUsageFee

    def createMessage(self):
        message = {
            "producerID": self.producerID,
            "consumerID": self.consumerID,
            "energyKwh": self.energyTraded,
            "energyPoolPrice": self.energyPoolPrice,
            "gridUsageFee": self.gridUsageFee
        }
        return message


# a template for a message exchanged between the pools
def createInterpoolMessage(sourceID, targetID, payloadType, message):
    entryToSend = {
        "originProsumerID": sourceID,
        "targetProsumerID": targetID,
        "targetComponentID": "PoolingPlatform",
        "payloadType": payloadType,
        "payload": message
    }
    return entryToSend


# ___________________________________________________________________________________________________________
# ------------------------------------COMMUNICATION----------------------------------------------------------
# ___________________________________________________________________________________________________________

class interPoolMessage(BaseModel): #from the admin pool to the member pool
    originProsumerID: str
    targetProsumerID: str
    targetComponentID: str = "PoolingPlatform"
    payloadType: str = "DummyJSON"
    payload: str

coordinator = {}
@poolingApp.post("/interPoolCommunication/{memberID}/tradeableEnergy/{poolID}/{targetMemberID}")
async def sendOwnTradeableEnergyToOtherPoolMembers(memberID: str, poolID: str, targetMemberID: str, message: interPoolMessage):
    index =  memberID + "_" + poolID + "_" + targetMemberID + "_TE"
    coordinator[index] = message
    return message

@poolingApp.get("/interPoolCommunication/{memberID}/tradeableEnergy/{poolID}/{targetMemberID}")
async def getTradeableEnergyFromOtherPoolMembers(memberID: str, poolID: str, targetMemberID: str):
    index =  memberID + "_" + poolID + "_" + targetMemberID + "_TE"
    if index in coordinator: #if the entry is not there yet
        return coordinator[index]
    else:
        # just send a non-participating message
        message = {"poolID": poolID, "memberID": memberID, "role": -11,
                   "tradeableEnergyInPool": 0}
        message = createInterpoolMessage(memberID, targetMemberID, 'tradeableEnergy', json.dumps(message))
        return message

@poolingApp.post("/interPoolCommunication/{memberID}/matches/{poolID}/{targetMemberID}")
async def sendMatchesToOtherPoolMembers(memberID: str, poolID: str, targetMemberID: str, message: interPoolMessage):
    index = memberID + "_" + poolID + "_" + targetMemberID + "_transaction"
    coordinator[index] = message
    return message

@poolingApp.get("/interPoolCommunication/{memberID}/matches/{poolID}/{targetMemberID}")
async def getMatchesFromOtherPoolMembers(memberID: str, poolID: str, targetMemberID: str):
    index = memberID + "_" + poolID + "_" + targetMemberID + "_transaction"
    if index in coordinator:  # if the entry is not there yet
        return coordinator[index]
    else:
        #return an empty transaction if there is no record yet
        matchWithTheMember = poolMatch(targetMemberID, memberID, 'none', 0, 0)
        matchMessage = json.dumps(matchWithTheMember.createMessage())
        message = createInterpoolMessage(memberID, targetMemberID, 'matches', matchMessage)
        return message

async def runOneSmallRound(roundNo, allMemberIDs):
    communityPoolingPlatforms = []
    coroutines = []

    for memberID in allMemberIDs:
        newPP = PoolingPlatform(memberID)
        communityPoolingPlatforms.append(newPP)
        coroutines.append(newPP.executePoolingPlatformRound())

    # one small round
    print(f'----------ROUND {roundNo}')
    outputs = await asyncio.gather(*coroutines)
    return outputs


for communityCondition in communityConditions:
    if debugCommunityCondition != '':
        communityCondition = debugCommunityCondition
    for tradingCondition in tradingConditions.keys():
        if debugTradingCondition != '':
            tradingCondition = debugTradingCondition

        for simulationRound in range(1, (noSimulationRounds + 1)):

            if dataset == 'debug':
                simulationRound = 0
            consNo, prosNo = communityCondition.split('_')
            noOfConsumers = int(consNo[4:])
            noOfProsumers = int(prosNo[4:])

            pooling = tradingConditions[tradingCondition][0]
            market = tradingConditions[tradingCondition][1]

            testingMode = tradingCondition

            # saving directories
            poolsLogDirRoot = f'testLogs/cons{noOfConsumers}_pros{noOfProsumers}/{testingMode}'
            communitySetupDir = f'testLogs/userPreferenceSetups/{testingMode}'

            # Create a dataframe with all of the consumers data
            consumers, memberNo = extractEnergyDataForRole('consumer', noOfConsumers, startDate, endDate, dataset)
            prosumers, memberNo = extractEnergyDataForRole('prosumer', noOfProsumers, startDate, endDate, dataset,
                                                           memberNo)

            if dataset == 'dataset1':
                allMembers = pd.concat([consumers, prosumers], axis=1)
            else:
                allMembers = consumers | prosumers

            allMemberIDs = list(allMembers)
            gridLocationsList = []
            for index in range(0, len(allMemberIDs)):
                gridLocationsList.append(index % 3)



            print(f'SIMULATION ROUND {simulationRound}')

            # create/update community pools DF
            communityPoolsDF = pd.read_csv(os.path.join(communitySetupDir, f'userPreferences_{simulationRound}.csv'), index_col='memberID')
            fakeCPDatabase = generatefakeCPDatabase(communityPoolsDF, allMemberIDs, gridLocationsList)

            #generate paths for saving
            poolsLogDir = os.path.join(poolsLogDirRoot, 'Simround_{}'.format(simulationRound))

            if os.path.exists(poolsLogDir):
                shutil.rmtree(poolsLogDir)

            os.makedirs(poolsLogDir)
            communityLog = os.path.join(poolsLogDir, 'communityLog.csv')

            #save the user preference table as csv
            # communityPoolsDF.to_csv(os.path.join(poolsLogDir, 'userPreferences.csv'))

            communityLogWithMarket = pd.DataFrame()
            commMetrics = []
            acceptanceRates = []

            for timeslotNo in range(0, noOfTimeslotsInRound):
                simTimestamp = ''
                fakeObsdDB = createFakeObsdDB(timeslotNo, dataset, allMembers)
                simulationLog = asyncio.run(runOneSmallRound(timeslotNo, allMemberIDs))
                simulationLog = pd.concat(simulationLog).reset_index()
                selfSuffCons, accRates = calculateAllMetricsPerRound(simulationLog, poolsLogDir, calculateAcceptance=True, poolOrMarket='pool', marketPrice=marketPrice, allMemberIDs=allMemberIDs)
                commMetrics.append(selfSuffCons)
                acceptanceRates.append(accRates)
                if market:
                    newCommunityLog = runDoubleAuctionRound(simulationLog, poolsLogDir)
                    selfSuffCons, accRates = calculateAllMetricsPerRound(newCommunityLog, poolsLogDir, calculateAcceptance=True, poolOrMarket='market', marketPrice=marketPrice, allMemberIDs=allMemberIDs)
                    commMetrics.append(selfSuffCons)
                    acceptanceRates.append(accRates)
                    communityLogWithMarket = pd.concat([communityLogWithMarket, newCommunityLog])
                    communityLogWithMarket.to_csv(os.path.join(poolsLogDir, 'communityLogWithMarket.csv'), index=False)
            commMetrics = pd.concat(commMetrics)
            if len(acceptanceRates[0]):
                acceptanceRates = pd.concat(acceptanceRates)
            # print('Round {}: Self-sufficiency: {}         Self-consumption: {}'.format(simulationRound, commMetrics['Self-sufficiency'].mean(), commMetrics['Self-consumption'].mean()))
            #
            #


