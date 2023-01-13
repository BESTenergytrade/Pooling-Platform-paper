import json
from typing import Optional, Set, List
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field
from datetime import date
from createCommunity import *
#simulate a fake database of participants and their pool sets


#pool information schema
class CPPool(BaseModel):
    poolID: str
    description: Optional[str] = None
    poolTitle: str
    userRole: int
    adminUserID: str
    userAdminRights: int
    userEnergyDistributionAsProducer: float
    userEnergyDistributionAsConsumer: float
    userGreenEnergyPreference: int
    userPricePolicyOption: int
    userPricePolicyFixedPrice: float
    userPricePolicyPercent: float
    participantIDs: list = []
    participantGridLocations: list = []
    participantAllowedRoles: list = []


#schema for a set of pools
class setOfPools(BaseModel):
    timeSlot: str
    targetComponentID: str
    userId: str
    pools: List[CPPool]



#generate the database with the information of pool memberships and energy distribution for each member
#for each member, it also contains the configuration for each pool that he/she is a member of
def generatefakeCPDatabase(communityPoolsDF, allMemberIDs, gridLocationsList):
    fakeCPDatabase = {}
    for memberID in allMemberIDs:
        # IDs may differ in pool allocation => get the mapping
        memidInd = allMemberIDs.index(memberID)
        indexID = communityPoolsDF.index[memidInd]

        #create the data structure
        poolsList = [
            CPPool(poolID="pool1",
                   poolTitle="Green Energy Pool",
                   userRole=2,  # 0 - consumer, 1 - producer, 2 - prosumer
                   adminUserID='',
                   userAdminRights=1,
                   userEnergyDistributionAsProducer=communityPoolsDF.loc[indexID, 'Pool1_Prod'],
                   userEnergyDistributionAsConsumer=communityPoolsDF.loc[indexID, 'Pool1_Cons'],
                   userGreenEnergyPreference=1,
                   userPricePolicyOption=2,  # 0 - market price, 1 - Festpreis, 2 - % of the market price
                   userPricePolicyFixedPrice=-1,
                   userPricePolicyPercent=120,
                   participantIDs=allMemberIDs,
                   participantGridLocations=gridLocationsList,
                   participantAllowedRoles=[1, 1, 1]),  # [consumer, producer, prosumer]
            CPPool(poolID="pool2",
                   poolTitle="Altruistic Sharing Pool",
                   userRole=2,  # 0 - consumer, 1 - producer, 2 - prosumer
                   adminUserID='',
                   userAdminRights=1,
                   userEnergyDistributionAsProducer=communityPoolsDF.loc[indexID, 'Pool2_Prod'],
                   userEnergyDistributionAsConsumer=communityPoolsDF.loc[indexID, 'Pool2_Cons'],
                   userGreenEnergyPreference=0,
                   userPricePolicyOption=2,  # 0 - market price, 1 - Festpreis, 2 - % of the market price
                   userPricePolicyFixedPrice=12,
                   userPricePolicyPercent=60,
                   participantIDs=allMemberIDs,
                   participantGridLocations=gridLocationsList,
                   participantAllowedRoles=[1, 1, 1]),  # [consumer, producer, prosumer]
            CPPool(poolID="pool3",
                   poolTitle="Family Pool1",
                   userRole=2,  # 0 - consumer, 1 - producer, 2 - prosumer
                   adminUserID='',
                   userAdminRights=1,
                   userEnergyDistributionAsProducer=communityPoolsDF.loc[indexID, 'Pool3_Prod'],
                   userEnergyDistributionAsConsumer=communityPoolsDF.loc[indexID, 'Pool3_Cons'],
                   userGreenEnergyPreference=0,
                   userPricePolicyOption=2,  # 0 - market price, 1 - Festpreis, 2 - % of the market price
                   userPricePolicyFixedPrice=12,
                   userPricePolicyPercent=0,
                   participantIDs=allMemberIDs,
                   participantGridLocations=gridLocationsList,
                   participantAllowedRoles=[1, 1, 1]),  # [consumer, producer, prosumer],
            CPPool(poolID="pool4",
                   poolTitle="Family Pool2",
                   userRole=2,  # 0 - consumer, 1 - producer, 2 - prosumer
                   adminUserID='',
                   userAdminRights=1,
                   userEnergyDistributionAsProducer=communityPoolsDF.loc[indexID, 'Pool4_Prod'],
                   userEnergyDistributionAsConsumer=communityPoolsDF.loc[indexID, 'Pool4_Cons'],
                   userGreenEnergyPreference=0,
                   userPricePolicyOption=2,  # 0 - market price, 1 - Festpreis, 2 - % of the market price
                   userPricePolicyFixedPrice=12,
                   userPricePolicyPercent=0,
                   participantIDs=allMemberIDs,
                   participantGridLocations=gridLocationsList,
                   participantAllowedRoles=[1, 1, 1])  # [consumer, producer, prosumer]
        ]

        setOfPoolss = setOfPools(timeSlot = str(date.today()), targetComponentID = 'PoolingPlatform', userId = memberID, pools = poolsList)
        fakeCPDatabase[memberID] = setOfPoolss
    return fakeCPDatabase


