import os


def participantIDMapping(memberIDs, communityCondition, dictorlist = 'list'):
    if dictorlist == 'list':
        idMappingList = []
    else:
        idMappingList = {}
    consNo, prosNo = communityCondition.split('_')
    consNo = int(consNo[4:])
    consumerIDs = memberIDs[:consNo]
    prosumerIDs = memberIDs[consNo:]


    for consumerID in consumerIDs:
        consIDNo = consumerIDs.index(consumerID) + 1

        if dictorlist == 'list':
            idMappingList.append(f'C{consIDNo}')
        else:
            idMappingList[consumerID] = f'C{consIDNo}'

    for prosumerID in prosumerIDs:
        prosIDNo = prosumerIDs.index(prosumerID) + 1

        if dictorlist == 'list':
            idMappingList.append(f'P{prosIDNo}')
        else:
            idMappingList[prosumerID] = f'P{prosIDNo}'

    return idMappingList

def cleanAllPreviousData(filename, communityConditions, tradingConditions):
    for communityCondition in communityConditions:
        for tradingCondition in tradingConditions:
            for simroundNo in range(1, 5):
                fileToDelete = f'./testLogs/{communityCondition}/{tradingCondition}/Simround_{simroundNo}/{filename}.csv'
                if os.path.exists(fileToDelete):
                    os.remove(fileToDelete)
