from libraryImports import *
from createCommunity import *

class obsdData(BaseModel):
    targetComponentID: str = "PoolingPlatform"
    userID: str
    timeStamp: str
    greyEnergyKwh: float
    greenEnergyKwh: float





fakeObsdDB = {}
def createFakeObsdDB(timeslotNo, dataset, allMembers):
    allMemberIDs = list(allMembers)
    for memberID in allMemberIDs:
        if memberID == 'H19':
            pass
        if dataset == "dataset2":
            memberInfo = allMembers[memberID]
            timestamp = memberInfo.index[timeslotNo]
            greyEnergy = memberInfo.loc[timestamp, 'CHP'] + memberInfo.loc[timestamp, 'Load']
            greenEnergy = memberInfo.loc[timestamp, 'PV']

        elif dataset == 'dataset1':
            greyEnergy = 0
            greenEnergy = allMembers[memberID][timeslotNo]
            timestamp = allMembers.index[timeslotNo]

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

pass

# fakeObsdDB = {
#     "participant0": obsdData(
#         targetComponentID = "PoolingPlatform",
#         userID = 'participant1',
#         timeStamp = str(date.today()),
#         energyKwh = -600,
#         gridRestrictions = [0, 0, 0, 0, 0, 0, 0, 0, 0]
#     ),
#     "participant1": obsdData(
#         targetComponentID = "PoolingPlatform",
#         userID = 'participant2',
#         timeStamp = str(date.today()),
#         energyKwh = 400,
#         gridRestrictions = [0, 0, 0, 0, 0, 0, 0, 0, 0]
#     ),
#     "participant2": obsdData(
#         targetComponentID = "PoolingPlatform",
#         userID = 'participant3',
#         timeStamp = str(date.today()),
#         energyKwh = 800,
#         gridRestrictions = [0, 0, 0, 0, 0, 0, 0, 0, 0]
#     ),
#     "participant3": obsdData(
#         targetComponentID = "PoolingPlatform",
#         userID = 'participant4',
#         timeStamp = str(date.today()),
#         energyKwh = 800,
#         gridRestrictions = [0, 0, 0, 0, 0, 0, 0, 0, 0]
#     ),
#
# }


def getObsdData(participantID):
    return fakeObsdDB[participantID]


