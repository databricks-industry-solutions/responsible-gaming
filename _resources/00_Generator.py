# Databricks notebook source
# MAGIC %pip install faker==13.15.0

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col
from faker import Faker
from faker.providers import internet, misc, date_time
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import random
import uuid
import json
import shutil

# COMMAND ----------

fake = Faker()

# COMMAND ----------

numCustomers = 500

# COMMAND ----------

########################################
# BUSINESS RULES FOR DATA GEN
########################################

customer_segments_df = pd.DataFrame(
  {'segment': ['A','B','C','D','E'],
   'segment_weight': [0.09, 0.16, 0.15, 0.20, 0.40]})

high_risk_prob_df = pd.DataFrame(
  {'segment': ['A','B','C','D','E'],
   'segment_weight': [0.19, 0.06, 0.10, 0.09, 0.01]})

age_band_df = pd.DataFrame(
  {'age_band': ['18-24', '25-34', '35-44', '45-54', '55-65', '65+'],
   'age_band_weight': [0.20, 0.22, 0.12, 0.17, 0.19, 0.10]})

gender_df = pd.DataFrame(
  {'gender': ['Male', 'Female', 'Unknown'],
   'gender_weight': [0.81, 0.17, 0.02]})

active_day_prob_df = pd.DataFrame(
  {'segment': ['A','B','C','D','E'],
   'active_day_prob': [0.36, 0.18, 0.14, 0.74, 0.10]}) 

sports_bet_prob_df = pd.DataFrame(
  {'segment': ['A','B','C','D','E'],
   'sports_bet_prob': [0.43, 0.03, 0.82, 0.91, 0.35]})

non_sports_games = ['Roulette','Video Poker', 'Slots', 'Baccarat','Blackjack', 'Craps']

daily_bets_max_df = pd.DataFrame(
   {'segment': ['A','B','C','D','E'],
    'max_daily_bets': [20, 80, 4, 6, 10]})

bet_wager_df = pd.DataFrame(
   {'segment': ['A','B','C','D','E'],
    'mean_bet': [10, 1, 20, 5, 3],
    'max_bet': [119, 23, 107, 32, 29]})

house_advantage_df = pd.DataFrame(
   {'game_type':['Roulette','Video Poker','Sports Betting','Slots','Baccarat','Blackjack','Craps'],
    'house_advantage': [0.056,0.048,0.045,0.033,0.017,0.008,0.006]})

withdrawal_prob_df = pd.DataFrame(
   {'segment': ['A','B','C','D','E'],
    'withdrawal_prob': [0.12, 0.09, 0.20, 0.10, 0.03]})

withdrawal_max_amount_df = pd.DataFrame(
   {'segment': ['A','B','C','D','E'],
    'max_withdrawal': [962, 245, 98, 200, 102]})

deposit_prob_df = pd.DataFrame(
   {'segment': ['A','B','C','D','E'],
    'deposit_prob': [0.65, 0.65, 0.75, 0.50, 0.20]})  # THESE VALUES SEEM TOO HIGH

deposit_max_amount_df = pd.DataFrame(
   {'segment': ['A','B','C','D','E'],
    'max_deposit': [200, 100, 50, 20, 25]})

last_month_active_df = pd.DataFrame(
  {'month': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
   'days_in_month': [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],     
   'A': [0.40, 0.17, 0.03, 0.02, 0.01, 0.01, 0.01, 0.02, 0.01, 0.01, 0.01, 0.30],
   'B': [0.42, 0.19, 0.03, 0.01, 0.01, 0.01, 0.01, 0.02, 0.03, 0.01, 0.01, 0.25],
   'C': [0.43, 0.18, 0.03, 0.02, 0.02, 0.03, 0.01, 0.02, 0.02, 0.02, 0.01, 0.21],
   'D': [0.45, 0.14, 0.03, 0.03, 0.03, 0.03, 0.02, 0.01, 0.02, 0.01, 0.01, 0.22],
   'E': [0.50, 0.13, 0.04, 0.03, 0.03, 0.02, 0.03, 0.01, 0.01, 0.01, 0.01, 0.18]})


# COMMAND ----------

########################################
# HELPER FUNCTIONS
########################################

def getRandomWeighted(df,vals,weights):
  return random.choices(list(df[vals]),list(df[weights]))[0]

########################################
# DATA GEN FUNCTIONS
########################################

def getCustomerId():
  return fake.uuid4()

def getCustomerSegment():
  return getRandomWeighted(customer_segments_df,'segment','segment_weight')

def getRegistrationDate():
  return '2021-01-01'

def getLastActiveDate(customerSegment):
  fmt = "%Y-%m-%d"
  last_month_active = getRandomWeighted(last_month_active_df,'month',customerSegment)
  num_days_in_month = last_month_active_df['days_in_month'][last_month_active-1]
  last_active_day_in_month = random.choices([i for i in range(1,num_days_in_month+1)])[0]
  last_active_date_str = "2021-{}-{}".format(last_month_active,last_active_day_in_month)
  return datetime.strptime(last_active_date_str, fmt).strftime(fmt)

def getHighRiskFlag(customerSegment):
  risk_prob = high_risk_prob_df[high_risk_prob_df['segment'] == customerSegment]['segment_weight'].item()
  return random.choices([0,1],[(1-risk_prob),risk_prob])[0]

def getAgeBand():
  return getRandomWeighted(age_band_df,'age_band','age_band_weight')

def getGender():
  return getRandomWeighted(gender_df,'gender','gender_weight')

def getListOfDays(registrationDate, lastActiveDate):
  fmt = "%Y-%m-%d"
  registrationDate_fmt = datetime.strptime(registrationDate, fmt) 
  lastActiveDate_fmt = datetime.strptime(lastActiveDate, fmt)
  
  delta = lastActiveDate_fmt - registrationDate_fmt
  
  listOfDays_fmt = [registrationDate_fmt + timedelta(days=i) for i in range(delta.days + 1)]
  
  return [i.strftime(fmt) for i in listOfDays_fmt]

def getIsActiveDayFlag(customerSegment):
  active_day_prob = active_day_prob_df[active_day_prob_df['segment'] == customerSegment]['active_day_prob'].item()
  return random.choices([0,1],[(1-active_day_prob),active_day_prob])[0]

def getGameType(customerSegment):
  sportsBetProb = sports_bet_prob_df[sports_bet_prob_df['segment'] == customerSegment]['sports_bet_prob'].item()
  isSportsBet = random.choices([0,1],[(1-sportsBetProb), sportsBetProb])[0]
  gameType = [random.choices(non_sports_games)[0] if isSportsBet == 1 else 'Sports Betting'][0]
  return gameType

def getNumDailyBets(customerSegment):
  dailyBetsMax = daily_bets_max_df[daily_bets_max_df['segment'] == customerSegment]['max_daily_bets'].item()
  #numBets = int(random.gauss(dailyBetsMean,3))
  numBets = round(random.triangular(1,dailyBetsMax,dailyBetsMax/2))
  return numBets  

def getBetWager(customerSegment):
  betWagerMean = bet_wager_df[bet_wager_df['segment'] == customerSegment]['mean_bet'].item()
  betWagerMax = bet_wager_df[bet_wager_df['segment'] == customerSegment]['max_bet'].item()
  betWager = round(random.triangular(1,betWagerMax,betWagerMean))
  return betWager

def getHouseAdvantage(gameType):
  return house_advantage_df[house_advantage_df['game_type'] == gameType]['house_advantage'].item()

def getBet(customerSegment):
  gameType = getGameType(customerSegment)
  wager = getBetWager(customerSegment)
  winFlag = None
  houseAdvantage = getHouseAdvantage(gameType)
  theoreticalLoss = round(houseAdvantage * wager,2)
  
  return {'gameType':gameType, 'wager':wager,'winFlag':winFlag,'houseAdvantage':houseAdvantage,'theoreticalLoss':theoreticalLoss}

def getIsWithdrawalDay(customerSegment):
  withdrawal_prob = withdrawal_prob_df[withdrawal_prob_df['segment'] == customerSegment]['withdrawal_prob'].item()
  return random.choices([0,1],[(1-withdrawal_prob),withdrawal_prob])[0]

def getWithdrawalAmount(customerSegment):
  withdrawalMax = withdrawal_max_amount_df[withdrawal_max_amount_df['segment'] == customerSegment]['max_withdrawal'].item()
  withdrawalAmt = round(random.triangular(10,withdrawalMax,withdrawalMax/2),2)
  return withdrawalAmt
  
def getIsDepositDay(customerSegment):
  deposit_prob = deposit_prob_df[deposit_prob_df['segment'] == customerSegment]['deposit_prob'].item()
  return random.choices([0,1],[(1-deposit_prob),deposit_prob])[0]

def getDepositAmount(customerSegment):
  depositMax = deposit_max_amount_df[deposit_max_amount_df['segment'] == customerSegment]['max_deposit'].item()
  depositAmt = round(random.triangular(5,depositMax,depositMax/2),2)
  return depositAmt

# COMMAND ----------

########################################
# CONSTRUCT BEACON FUNCTIONS
########################################

def getEmptyBeacon():
  emptyBeacon = {
    'customerId': None,
    'date': None,
    'dateBetNumber': None,
    'eventType': None,
    'gameType': None,
    'wager': None,
    'winFlag': None,
    'theoreticalLoss': None,
    'ageBand': None,
    'gender': None,
    'withdrawalAmount': None,
    'depositAmount': None
  }
  return emptyBeacon

def getRegistrationBeacon(customerId, registrationDate, ageBand, gender):
  beacon = getEmptyBeacon()
  beacon['customerId'] = customerId
  beacon['date'] = registrationDate
  beacon['eventType'] = 'register'
  beacon['ageBand'] = ageBand
  beacon['gender'] = gender
  
  return '\n'+json.dumps(beacon)

def getFlaggedHighRiskBeacon(customerId, lastActiveDate):
  beacon = getEmptyBeacon()
  beacon['customerId'] = customerId
  beacon['date'] = lastActiveDate
  beacon['eventType'] = 'flaggedHighRisk'
  
  return '\n'+json.dumps(beacon)

def getBetBeacon(customerId,date,dateBetNum, bet):
  beacon = getEmptyBeacon()
  beacon['customerId'] = customerId
  beacon['date'] = date
  beacon['dateBetNumber'] = dateBetNum
  beacon['eventType'] = 'bet'
  beacon['gameType'] = bet['gameType']
  beacon['wager'] = bet['wager']
  beacon['winFlag'] = bet['winFlag']
  beacon['theoreticalLoss'] = bet['theoreticalLoss']
  
  return '\n'+json.dumps(beacon)

def getWithdrawalBeacon(customerId,date,withdrawalAmount):
  beacon = getEmptyBeacon()
  beacon['customerId'] = customerId
  beacon['date'] = date
  beacon['eventType'] = 'withdrawal'
  beacon['withdrawalAmount'] = withdrawalAmount
  
  return '\n'+json.dumps(beacon)

def getDepositBeacon(customerId,date,depositAmount):
  beacon = getEmptyBeacon()
  beacon['customerId'] = customerId
  beacon['date'] = date
  beacon['eventType'] = 'deposit'
  beacon['depositAmount'] = depositAmount
  
  return '\n'+json.dumps(beacon)

# COMMAND ----------

# !!! Uncomment me to do full refresh
# dbutils.fs.rm('dbfs:/databricks_solacc/real_money_gaming/data/raw', True)
dbutils.fs.mkdirs('dbfs:/databricks_solacc/real_money_gaming/data/raw')

# COMMAND ----------

customerCounter = 0

for c in range(numCustomers):
  file_name = uuid.uuid4()
  
  with open("/dbfs/databricks_solacc/real_money_gaming/data/raw/{}".format(file_name), 'w') as f:
    customerId = getCustomerId()
    customerSegment = getCustomerSegment()
    registrationDate = getRegistrationDate()
    lastActiveDate = getLastActiveDate(customerSegment)
    flaggedHighRisk = getHighRiskFlag(customerSegment) 
    ageBand = getAgeBand() # maybe update to vary based on segment
    gender = getGender()
  
  #### GENERATE AND WRITE REGISTRATION BEACONS ####
  
    registrationBeacon = getRegistrationBeacon(customerId, registrationDate, ageBand, gender)
    f.writelines(registrationBeacon)
  
  #### GENERATE AND WRITE FLAGGED HIGH RISK BEACONS ####
  
    if flaggedHighRisk == 1:
      flaggedHighRiskBeacon = getFlaggedHighRiskBeacon(customerId, lastActiveDate)
      f.writelines(flaggedHighRiskBeacon)
  
  #### ITERATE THROUGH CUSTOMER LIFECYCLE DAYS ####
  
    for date in getListOfDays(registrationDate,lastActiveDate): #list of days between registration and last active
      isActiveDay = getIsActiveDayFlag(customerSegment)
    
      if isActiveDay == 1:
        numBets = getNumDailyBets(customerSegment)
        isWithdrawalDay = getIsWithdrawalDay(customerSegment)
        isDepositDay = getIsDepositDay(customerSegment)
  
  #### GENERATE AND WRITE BET BEACONS FOR A GIVEN DAY ####
    
        for betNum in range(numBets):
          dateBetNum = betNum + 1
          bet = getBet(customerSegment)
          betBeacon = getBetBeacon(customerId, date, dateBetNum, bet)
          f.writelines(betBeacon)
  
  #### GENERATE WITHDRAWAL BEACONS FOR A GIVEN DAY ####
        if isWithdrawalDay == 1:
          withdrawalAmount = getWithdrawalAmount(customerSegment)
          withdrawalBeacon = getWithdrawalBeacon(customerId,date,withdrawalAmount)
          f.writelines(withdrawalBeacon)
  
  #### GENERATE DEPOSIT BEACONS FOR A GIVEN DAY ####
        if isDepositDay == 1:
          depositAmount = getDepositAmount(customerSegment)
          depositBeacon = getDepositBeacon(customerId,date,depositAmount)
          f.writelines(depositBeacon)
  
  #### INCREMENT AND PRINT COUNTER
    customerCounter +=1
    if customerCounter % 100 == 0:
      print("Completed {} customers".format(customerCounter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checks

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks_solacc/real_money_gaming/data/raw')

# COMMAND ----------

display(spark.read.json('dbfs:/databricks_solacc/real_money_gaming/data/raw'))
