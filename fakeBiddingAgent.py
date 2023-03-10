from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field
from datetime import date

biddingAgentApp = FastAPI()
poolingApp = FastAPI()

fakeBiddingAgent = {}
marketPrice = 33
class leftoverEnergy(BaseModel):
    memberID: str
    unmatchedEnergyInKwh: float

def getMarketPrice():
    marketData = {'marketPriceEurocentPerKwh': marketPrice}
    return marketData

@poolingApp.post('/leftoverEnergy/', response_model=leftoverEnergy)
async def sendUnmatchedEnergy(item: leftoverEnergy):
    fakeBiddingAgent[item.memberID] = item
    return item



