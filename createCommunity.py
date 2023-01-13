import os

import pandas as pd
from globalVars import *
from libraryImports import *
baseIP = "base"
#generate community
memberPorts = {}

generateNewCommunitySetups = True
noOfNewUserPrefSetups = 4

if not os.path.exists('testLogs'):
    os.makedirs('testLogs')

#extract energy data for a required number of members belonging to the specified role (prosumer/consumer)
#also within the specified date range
#outputs a formatted dataframe with the energy info of all members with the specified requirements
#if the role is consumer, outputs only demand data for this set of users
#for prosumers, defines the role for each timestamp (i.e. whether at that timepoint there is more demand or supply)
def extractEnergyDataForRole(role, roleCount, startDate, endDate, dataset, startingMemberNo=1, disregardTIme=True):
    energyDataPath = f'data/{dataset}/'

    if dataset == 'dataset2':
        roleMembers = {}
        memberNo = startingMemberNo

        #check that the data for this role is extant

        startDateShort = startDate[0:(len(startDate) - 3)]

        month = datetime.strptime(startDateShort, '%Y-%m-%d').strftime('%B')[0:3]
        energyTypes = ['PV', 'CHP', 'Load']

        while len(roleMembers) < roleCount:
            memberID = f'H{memberNo}'
            memberData = pd.DataFrame()

            dataForRoleExists = True
            for energyType in energyTypes:
                datapath = os.path.join(energyDataPath, energyType, month, f'{energyType}_{month}_{memberNo}.csv')
                if os.path.exists(datapath):
                    data = pd.read_csv(datapath)
                    data[memberID] = data[memberID] / 1000 #convert to kwh
                    if not disregardTIme:
                        startInd = data.index[data['Datetime'].str.startswith(startDate)][0]
                        endInd = data.index[data['Datetime'].str.startswith(endDate)][0]
                        data = data.iloc[startInd:endInd]
                    if len(memberData) == 0:
                        memberData = data
                        memberData['Interval'] = memberData['Datetime']  # copy the datetime column
                        memberData['Datetime'] = pd.to_datetime(memberData['Datetime'])
                        memberData = memberData.rename(columns={memberID: energyType})
                        memberData = memberData.set_index('Datetime')
                    else:
                        data['Datetime'] = pd.to_datetime(data['Datetime'])
                        data = data.set_index('Datetime')
                        data = data.rename(columns={memberID: energyType})
                        memberData = pd.concat([memberData, data], axis=1)
                else:
                    #if this member does not have the data for his role
                    if (role == 'consumer') & (energyType == 'Load'):
                        memberNo += 1
                        dataForRoleExists = False
                        break
                    elif (role == 'prosumer') & (energyType == 'PV') & (~os.path.exists(os.path.join(energyDataPath, 'CHP', month, f'CHP_{month}_{memberNo}.csv'))):
                        memberNo += 1
                        dataForRoleExists = False
                        break
                    memberData[energyType] = 0

            if dataForRoleExists:
                memberData['Load'] = memberData['Load'] * (-1)

                if 'PV' not in memberData.columns:
                    memberData['PV'] = 0

                if role == 'consumer':
                    memberData['PV'] = 0
                    memberData['CHP'] = 0

                #calculate available energy
                #spend the PV energy first
                energyAfterTransaction = memberData['PV'] + memberData['Load']

                memberData['PV'] = energyAfterTransaction
                memberData['Load'] = energyAfterTransaction

                #spend CHP energy
                secondTransaction = memberData.loc[memberData['PV'] < 0, 'CHP'] + memberData.loc[memberData['PV'] < 0, 'Load']
                memberData.loc[memberData['PV'] < 0, 'CHP'] = secondTransaction
                memberData.loc[memberData['PV'] < 0, 'Load'] = secondTransaction

                #clean up the records
                memberData.loc[memberData['PV'] < 0, 'PV'] = 0
                memberData.loc[memberData['CHP'] < 0, 'CHP'] = 0
                memberData.loc[memberData['Load'] > 0, 'Load'] = 0

                roleMembers[memberID] = memberData

                memberNo += 1

    elif dataset == 'debug':
        if role == 'consumer':
            roleMembers = {'H1': pd.DataFrame({'PV': [0], 'CHP': [0], 'Load': [-0.1736]}),
                           'H2': pd.DataFrame({'PV': [0], 'CHP': [0], 'Load': [-0.1629]}),
                           'H3': pd.DataFrame({'PV': [0], 'CHP': [0], 'Load': [-0.23022]}),
                           'H4': pd.DataFrame({'PV': [0], 'CHP': [0], 'Load': [-0.1093]}),
                           'H5': pd.DataFrame({'PV': [0], 'CHP': [0], 'Load': [-0.5206]}),
                           'H6': pd.DataFrame({'PV': [0], 'CHP': [0], 'Load': [-0.0405]}),
                           'H7': pd.DataFrame({'PV': [0], 'CHP': [0], 'Load': [-0.0825]}),
                           'H8': pd.DataFrame({'PV': [0], 'CHP': [0], 'Load': [-0.1817]}),
                           'H10': pd.DataFrame({'PV': [0], 'CHP': [0], 'Load': [-0.0625]}),
                           'H11': pd.DataFrame({'PV': [0], 'CHP': [0], 'Load': [-0.0450]})
                           }
        elif role == 'prosumer':
            roleMembers = {'H12': pd.DataFrame({'PV': [2], 'CHP': [5], 'Load': [-0.4972]}),
                           'H13': pd.DataFrame({'PV': [4], 'CHP': [4], 'Load': [-0.2875]}),
                           'H14': pd.DataFrame({'PV': [4], 'CHP': [3], 'Load': [-0.1474]}),
                           'H15': pd.DataFrame({'PV': [7], 'CHP': [2], 'Load': [-0.5205]}),
                           'H16': pd.DataFrame({'PV': [13], 'CHP': [1], 'Load': [-0.0070]}),
                           'H17': pd.DataFrame({'PV': [2], 'CHP': [5], 'Load': [-0.3042]}),
                           'H18': pd.DataFrame({'PV': [4], 'CHP': [4], 'Load': [-0.1855]}),
                           'H19': pd.DataFrame({'PV': [4], 'CHP': [3], 'Load': [0.0282]}),
                           'H20': pd.DataFrame({'PV': [7], 'CHP': [2], 'Load': [-0.0828]}),
                           'H21': pd.DataFrame({'PV': [13], 'CHP': [1], 'Load': [-0.2678]})
                           }
        memberNo = 0


    return roleMembers, memberNo

#generate user preferences depending on the setup
def createCommunityPoolAllocation(allMemberIDs, market, pooling):
        communityPoolsDF = pd.DataFrame()
        for memberID in allMemberIDs:
            memberPosition = allMemberIDs.index(memberID) + 1
            if market & pooling:
                #generate consumer allocations
                pool1_cons = random.randint(0, 100)
                pool2_cons = random.randint(0, (100 - pool1_cons))
                if memberPosition % 5 == 0:
                    pool3_cons = random.randint(0, (100 - pool1_cons - pool2_cons))
                else:
                    pool3_cons = 0

                if memberPosition % 6 == 0:
                    pool4_cons = random.randint(0, (100 - pool1_cons - pool2_cons - pool3_cons))
                else:
                    pool4_cons = 0

                market_cons = 100 - pool1_cons - pool2_cons - pool3_cons - pool4_cons

                #generate producer allocations
                pool1_prod = random.randint(0, 100)
                pool2_prod = random.randint(0, (100 - pool1_prod))

                if memberPosition % 5 == 0:
                    pool3_prod = random.randint(0, (100 - pool1_prod - pool2_prod))
                else:
                    pool3_prod = 0

                if memberPosition % 6 == 0:
                    pool4_prod = random.randint(0, (100 - pool1_prod - pool2_prod - pool3_prod))
                else:
                    pool4_prod = 0

                market_prod = 100 - pool1_prod - pool2_prod - pool3_prod - pool4_prod
            elif pooling & ~market:
                # generate consumer allocations
                pool1_cons = random.randint(0, 100)
                if memberPosition % 5 == 0:
                    pool2_cons = random.randint(0, (100 - pool1_cons))
                    pool3_cons = 100 - pool1_cons - pool2_cons
                    pool4_cons = 0
                elif memberPosition % 6 == 0:
                    pool2_cons = random.randint(0, (100 - pool1_cons))
                    pool3_cons = 0
                    pool4_cons = 100 - pool1_cons - pool2_cons
                else:
                    pool2_cons = 100 - pool1_cons
                    pool3_cons = 0
                    pool4_cons = 0

                market_cons = 0

                # generate producer allocations
                pool1_prod = random.randint(0, 100)
                if memberPosition % 5 == 0:
                    pool2_prod = random.randint(0, (100 - pool1_prod))
                    pool3_prod = 100 - pool1_prod - pool2_prod
                    pool4_prod = 0
                elif memberPosition % 6 == 0:
                    pool2_prod = random.randint(0, (100 - pool1_prod))
                    pool3_prod = 0
                    pool4_prod = 100 - pool1_prod - pool2_prod
                else:
                    pool2_prod = 100 - pool1_prod
                    pool3_prod = 0
                    pool4_prod = 0

                market_prod = 0

            elif ~pooling & market:
                # generate consumer allocations
                pool1_cons = 0
                pool2_cons = 0
                pool3_cons = 0
                pool4_cons = 0
                market_cons = 100

                # generate producer allocations
                pool1_prod = 0
                pool2_prod = 0
                pool3_prod = 0
                pool4_prod = 0
                market_prod = 100

            line = pd.DataFrame({'memberID': memberID,
                                 'Pool1_Cons': pool1_cons, 'Pool1_Prod': pool1_prod,
                                 'Pool2_Cons': pool2_cons, 'Pool2_Prod': pool2_prod,
                                 'Pool3_Cons': pool3_cons, 'Pool3_Prod': pool3_prod,
                                 'Pool4_Cons': pool4_cons, 'Pool4_Prod': pool4_prod,
                                 'Market_Cons': market_cons, 'Market_Prod': market_prod
                                }, index=[memberID])
            communityPoolsDF = pd.concat([communityPoolsDF, line])

        return communityPoolsDF



#loops through trading conditions to trigger user preference setup generation
def generateNewCommunitySetupsFunc(noIds):
    tradingConditions = [[True, True], [False, True], [True, False]]
    allMemberIDs = []
    for idNo in range(noIds):
        allMemberIDs.append(f'H{idNo}')

    for conditionNo in range(0, 3):
        pooling = tradingConditions[conditionNo][0]
        market = tradingConditions[conditionNo][1]

        if pooling & market:
            testingMode = "poolingAndMarket"
        elif pooling & ~market:
            testingMode = "poolingOnly"
        elif ~pooling & market:
            testingMode = "marketOnly"

        communitySetupDir = f'testLogs/userPreferenceSetups/{testingMode}'

        saveDir = communitySetupDir

        if os.path.exists(saveDir):
            shutil.rmtree(saveDir)

        os.makedirs(saveDir)

        for setupNo in range(noOfNewUserPrefSetups):
            newSetup = createCommunityPoolAllocation(allMemberIDs, market, pooling)
            newSetup.to_csv(os.path.join(saveDir, f'userPreferences_{setupNo+1}.csv'))


if generateNewCommunitySetups:
    consNo, prosNo = communityConditions[0].split('_')
    noOfConsumers = int(consNo[4:])
    noOfProsumers = int(prosNo[4:])

    noIDs = noOfProsumers + noOfConsumers

    generateNewCommunitySetupsFunc(noIDs)


