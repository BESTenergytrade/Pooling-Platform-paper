from libraryImports import *
from createCommunity import *

#data schema
class obsdData(BaseModel):
    targetComponentID: str = "PoolingPlatform"
    userID: str
    timeStamp: str
    greyEnergyKwh: float
    greenEnergyKwh: float





fakeObsdDB = {}

#create a database with energy information for all the members for the given timeslot
def createFakeObsdDB(timeslotNo, dataset, allMembers):
    allMemberIDs = list(allMembers)
    for memberID in allMemberIDs:
        if dataset == "dataset2":
            memberInfo = allMembers[memberID]
            timestamp = memberInfo.index[timeslotNo]
            greyEnergy = memberInfo.loc[timestamp, 'CHP'] + memberInfo.loc[timestamp, 'Load']
            greenEnergy = memberInfo.loc[timestamp, 'PV']


        elif dataset == 'debug':
            memberInfo = allMembers[memberID]
            greyEnergy = memberInfo.loc[timeslotNo, 'CHP'] + memberInfo.loc[timeslotNo, 'Load']
            greenEnergy = memberInfo.loc[timeslotNo, 'PV']
            timestamp = '2019-09-01T08'

        dataItem = obsdData(
            targetComponentID = "PoolingPlatform",
            userID = memberID,
            timeStamp = str(timestamp),
            greyEnergyKwh = greyEnergy,
            greenEnergyKwh = greenEnergy
        )
        fakeObsdDB[memberID] = dataItem
    return fakeObsdDB


#instead of REST API used in the full version
def getObsdData(participantID):
    return fakeObsdDB[participantID]


