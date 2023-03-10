# Databricks notebook source
# MAGIC %md
# MAGIC * TO DO
# MAGIC   * Add missing game: Baccarat
# MAGIC   * Add missing game: Craps
# MAGIC   * Add missing game: Sports Betting
# MAGIC   * Make wagers more consistent across bets (e.g. persist amount across day/game/session)
# MAGIC   * Update terms used for wager_amount, win_loss, win_losos_amount in becaons and functions for consistency
# MAGIC   * Remove hardcoding for database/table name

# COMMAND ----------

# MAGIC %pip install faker==13.15.0

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col
from faker import Faker
from faker.providers import internet, misc, date_time
from datetime import datetime, timedelta
from collections import Counter
import numpy as np
import pandas as pd
import random
import uuid
import json
import shutil

# COMMAND ----------

fake = Faker()

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
# CONSTRUCT BEACON FUNCTIONS
########################################

def get_empty_beacon():
  empty_beacon = {
    'customer_id': None,
    'age_band': None,
    'gender': None,
    'date': None,
    'date_transaction_id': None,
    'event_type': None,
    'game_type': None,
    'wager_amount': None,
    'win_loss': None,
    'win_loss_amount': None,
    'initial_balance': None,
    'ending_balance': None,
    'withdrawal_amount': None,
    'deposit_amount': None
  }
  return empty_beacon

def get_registration_beacon(customer_id, registration_date, age_band, gender):
  beacon = get_empty_beacon()
  beacon['customer_id'] = customer_id
  beacon['date'] = registration_date
  beacon['event_type'] = 'register'
  beacon['age_band'] = age_band
  beacon['gender'] = gender
  return beacon

def get_flagged_high_risk_beacon(customer_id, last_active_date):
  beacon = get_empty_beacon()
  beacon['customer_id'] = customer_id
  beacon['date'] = last_active_date
  beacon['event_type'] = 'flagged_high_risk'
  return beacon

def get_bet_beacon(customer_id,date,date_transaction_id,game_type,wager_amount,win_loss,win_loss_amount,initial_balance,ending_balance):
  beacon = get_empty_beacon()
  beacon['customer_id'] = customer_id
  beacon['date'] = date
  beacon['date_transaction_id'] = date_transaction_id
  beacon['event_type'] = 'bet'
  beacon['game_type'] = game_type
  beacon['wager_amount'] = wager_amount
  beacon['win_loss'] = win_loss
  beacon['win_loss_amount'] = win_loss_amount
  beacon['initial_balance'] = initial_balance
  beacon['ending_balance'] = ending_balance
  return beacon

def get_deposit_beacon(customer_id,date,date_transaction_id,deposit_amount,initial_balance,ending_balance):
  beacon = get_empty_beacon()
  beacon['customer_id'] = customer_id
  beacon['date'] = date
  beacon['date_transaction_id'] = date_transaction_id
  beacon['event_type'] = 'deposit'
  beacon['deposit_amount'] = deposit_amount
  beacon['initial_balance'] = initial_balance
  beacon['ending_balance'] = ending_balance
  return beacon

def get_withdrawal_beacon(customer_id,date,date_transaction_id,withdrawal_amount,initial_balance,ending_balance):
  beacon = get_empty_beacon()
  beacon['customer_id'] = customer_id
  beacon['date'] = date
  beacon['date_transaction_id'] = date_transaction_id
  beacon['event_type'] = 'withdrawal'
  beacon['withdrawal_amount'] = withdrawal_amount
  beacon['initial_balance'] = initial_balance
  beacon['ending_balance'] = ending_balance
  return beacon

# COMMAND ----------

########################################
# HELPER FUNCTIONS
########################################

def _get_random_weighted(df,vals,weights):
  return random.choices(list(df[vals]),list(df[weights]))[0]

########################################
# CUSTOMERS
########################################

def get_customer_id():
  return fake.uuid4()

def get_customer_segment():
  return _get_random_weighted(customer_segments_df,'segment','segment_weight')

def get_registration_date():
  return '2021-01-01'

def get_last_active_date(customer_segment):
  fmt = "%Y-%m-%d"
  last_month_active = _get_random_weighted(last_month_active_df,'month',customer_segment)
  num_days_in_month = last_month_active_df['days_in_month'][last_month_active-1]
  last_active_day_in_month = random.choices([i for i in range(1,num_days_in_month+1)])[0]
  last_active_date_str = "2021-{}-{}".format(last_month_active,last_active_day_in_month)
  return datetime.strptime(last_active_date_str, fmt).strftime(fmt)
  
def get_high_risk_flag(customer_segment):
  risk_prob = high_risk_prob_df[high_risk_prob_df['segment'] == customer_segment]['segment_weight'].item()
  return random.choices([0,1],[(1-risk_prob),risk_prob])[0]
  
def get_age_band():
  return _get_random_weighted(age_band_df,'age_band','age_band_weight')

def get_gender():
  return _get_random_weighted(gender_df,'gender','gender_weight')

########################################
# SESSIONS
########################################

def _get_list_of_days(registration_date, last_active_date):
  fmt = "%Y-%m-%d"
  registration_date_fmt = datetime.strptime(registration_date, fmt) 
  last_active_date_fmt = datetime.strptime(last_active_date, fmt)
  
  delta = last_active_date_fmt - registration_date_fmt
  
  list_of_days_fmt = [registration_date_fmt + timedelta(days=i) for i in range(delta.days + 1)]

  return [i.strftime(fmt) for i in list_of_days_fmt]
  
def _get_is_active_day_flag(customer_segment):
  active_day_prob = active_day_prob_df[active_day_prob_df['segment'] == customer_segment]['active_day_prob'].item()
  return random.choices([0,1],[(1-active_day_prob),active_day_prob])[0]

def get_list_of_active_days(customer_segment, registration_date, last_active_date):
  list_of_days = _get_list_of_days(registration_date, last_active_date)
  list_of_active_day_flags = [_get_is_active_day_flag(customer_segment) for date in list_of_days]
  return [date for (date,val) in tuple(zip(list_of_days,list_of_active_day_flags)) if val == 1]

########################################
# BETS
########################################

def _get_num_daily_bets(customer_segment):
  daily_bets_max = daily_bets_max_df[daily_bets_max_df['segment'] == customer_segment]['max_daily_bets'].item()
  num_bets = round(random.triangular(1,daily_bets_max,daily_bets_max/2))
  return num_bets
  
def _get_game_type(customer_segment):
  sports_bet_prob = sports_bet_prob_df[sports_bet_prob_df['segment'] == customer_segment]['sports_bet_prob'].item()
  is_sports_bet = random.choices([0,1],[(1-sports_bet_prob), sports_bet_prob])[0]
  game_type = [random.choices(non_sports_games)[0] if is_sports_bet == 1 else 'Sports Betting'][0]
  return game_type

def get_daily_bets_by_game_type(customer_segment):
  num_bets = _get_num_daily_bets(customer_segment)
  bets_list = list()
  bets_dict = dict(Counter([_get_game_type(customer_segment) for bet in range(num_bets)]))
  for key in bets_dict.keys():
    _ = [bets_list.append(key) for i in range(num_bets)]
  bets_list.append('done')
  return bets_list

########################################
# WAGERS
########################################

def get_bet_wager(customer_segment):
  bet_wager_mean = bet_wager_df[bet_wager_df['segment'] == customer_segment]['mean_bet'].item()
  bet_wager_max = bet_wager_df[bet_wager_df['segment'] == customer_segment]['max_bet'].item()
  bet_wager = round(random.triangular(1,bet_wager_max,bet_wager_mean))
  bet_wager = int(round(bet_wager/5)*5) if (bet_wager > 2) else int(round(bet_wager))
  return float(bet_wager)

########################################
# DEPOSITS & WITHDRAWALS
########################################

def get_deposit_amount(customer_segment):
  deposit_max = deposit_max_amount_df[deposit_max_amount_df['segment'] == customer_segment]['max_deposit'].item()
  deposit_amount = round(random.triangular(5,deposit_max,deposit_max/2),2)
  deposit_amount = int(round(deposit_amount/10)*10) if (deposit_amount >= 10) else 5
  return float(deposit_amount)

def get_is_withdrawal_day(customer_segment):
  withdrawal_prob = withdrawal_prob_df[withdrawal_prob_df['segment'] == customer_segment]['withdrawal_prob'].item()
  return random.choices([0,1],[(1-withdrawal_prob),withdrawal_prob])[0]

# Not currently in use; will need to change to min to use
def getWithdrawalAmount(customer_segment):
  withdrawal_max = withdrawal_max_amount_df[withdrawal_max_amount_df['segment'] == customer_segment]['max_withdrawal'].item()
  withdrawal_amount = round(random.triangular(10,withdrawal_max,withdrawal_max/2),2)
  return float(withdrawal_amount)

########################################
# Games
########################################
class Blackjack:
  def __init__(self):
    pass
  
  def _get_win_loss(self):
    return random.choices(['win','loss','tie'],[0.42, 0.49, 0.09])[0]
  
  def _get_is_blackjack(self):
    return random.choices([0,1],[1-0.0475,0.0475])[0]
  
  def _get_payout(self,wager, win_loss,is_blackjack):
    if win_loss == 'win' and is_blackjack == 1:
      return (3/2 * wager) + wager
    elif win_loss == 'win' and is_blackjack == 0:
      return (1 * wager) + wager
    elif win_loss == 'tie':
      return 0
    else:
      return -wager
  
  def play(self,wager):
    win_loss = self._get_win_loss()
    is_blackjack = self._get_is_blackjack()
    payout = self._get_payout(wager, win_loss, is_blackjack)
    return {'game_type': 'blackjack', 'wager': wager, 'win_loss': win_loss, 'is_blackjack': is_blackjack, 'win_loss_amount': float(payout)}

###   
class Roulette:
  def __init__(self):
    self.rules = {"straight_up":     {"freq_of_bet": 0.03, "win_prob": 0.0260, "payout": 35/1},
                  "split":           {"freq_of_bet": 0.02, "win_prob": 0.0530, "payout": 17/1}, 
                  "trio":            {"freq_of_bet": 0.02, "win_prob": 0.0526, "payout": 11/1},
                  "street":          {"freq_of_bet": 0.03, "win_prob": 0.0790, "payout": 11/1},
                  "corner":          {"freq_of_bet": 0.03, "win_prob": 0.1050, "payout": 8/1},
                  "five_number_bet": {"freq_of_bet": 0.04, "win_prob": 0.1320, "payout": 6/1},
                  "line":            {"freq_of_bet": 0.04, "win_prob": 0.1580, "payout": 5/1},
                  "snake_bet":       {"freq_of_bet": 0.13, "win_prob": 0.3158, "payout": 2/1},
                  "column":          {"freq_of_bet": 0.12, "win_prob": 0.3160, "payout": 2/1},
                  "dozen":           {"freq_of_bet": 0.13, "win_prob": 0.3160, "payout": 2/1},
                  "high_low":        {"freq_of_bet": 0.13, "win_prob": 0.4737, "payout": 1/1},
                  "even_odd":        {"freq_of_bet": 0.12, "win_prob": 0.4737, "payout": 1/1},
                  "red_black":       {"freq_of_bet": 0.14, "win_prob": 0.4737, "payout": 1/1}}
  
  def _get_win_loss(self,wager_type):
    win_prob = self.rules[wager_type]['win_prob']
    return random.choices(['win','loss'],[win_prob,1-win_prob])[0]
  
  def _get_payout(self,wager,wager_type,win_loss):
    if win_loss == 'win':
      payout = self.rules[wager_type]['payout']
      return (payout * wager) + wager
    else:
      return -wager
    
  def play(self,wager):
    wager_type = random.choices(list(self.rules.keys()),[self.rules[x]['freq_of_bet'] for x in self.rules.keys()])[0]
    win_loss = self._get_win_loss(wager_type)
    payout = self._get_payout(wager,wager_type,win_loss)
    return {'game_type': 'roulette', 'wager': wager, 'wager_type': wager_type, 'win_loss': win_loss, 'win_loss_amount': float(payout)}

###
class Slots:
  def __init__(self):
    pass
  
  def _get_win_loss(self):
    return random.choices(['win','loss'],[0.26,0.74])[0]
  
  def _get_payout(self,wager,win_loss):
      if win_loss == 'win':
        multiplier = random.choices([1,2,3,4,5,6,7],[0.01,0.02,0.02,0.20,0.20,0.35,0.20])[0]
        return (multiplier * wager) + wager
      else:
        return -wager
  
  def play(self, wager):
    win_loss = self._get_win_loss()
    payout = self._get_payout(wager,win_loss)
    return {'game_type': 'slots', 'wager': wager, 'win_loss': win_loss, 'win_loss_amount': float(payout)}

###
class VideoPoker:
  def __init__(self):
    self.rules = {
      "royal_flush": {"win_prob": 0.002, "payout": 500},
      "straight_flush": {"win_prob": 0.002, "payout": 50},
      "four_of_a_kind": {"win_prob": 0.003, "payout": 25},
      "full_house": {"win_prob": 0.003, "payout": 9},
      "flush": {"win_prob": 0.01, "payout": 6},
      "straight": {"win_prob": 0.01, "payout": 4},
      "three_of_a_kind": {"win_prob": 0.07, "payout": 3},
      "two_pair": {"win_prob": 0.13, "payout": 2},
      "jacks_or_better": {"win_prob": 0.22, "payout": 1},
      "other": {"win_prob": 0.55, "payout": 0},
    }
  
  def _get_win_loss(self,hand_type):
    return 'lose' if hand_type == 'other' else 'win'
  
  def _get_payout(self,wager,wager_type,win_loss):
    if win_loss == 'win':
      payout = self.rules[wager_type]['payout']
      return (payout * wager) + wager
    else:
      return -wager
    
  def play(self,wager):
    hand_type = random.choices(list(self.rules.keys()),[self.rules[x]['win_prob'] for x in self.rules.keys()])[0]
    win_loss = self._get_win_loss(hand_type)
    payout = self._get_payout(wager,hand_type,win_loss)
    return {'game_type': 'video poker', 'wager': wager, 'win_loss': win_loss, 'win_loss_amount': float(payout)}

# COMMAND ----------

# DUMMY CLASSES

class Baccarat:
  def __init__(self):
    pass
  
  def play(self,wager):
    win_loss = 'win'
    payout = -1
    return {'game_type': 'baccarat', 'wager': wager, 'win_loss': win_loss, 'win_loss_amount': float(payout)}
  
class Craps:
  def __init__(self):
    pass
  
  def play(self,wager):
    win_loss = 'win'
    payout = -1
    return {'game_type': 'craps', 'wager': wager, 'win_loss': win_loss, 'win_loss_amount': float(payout)}

class SportsBetting:
  def __init__(self):
    pass
  
  def play(self,wager):
    win_loss = 'win'
    payout = -1
    return {'game_type': 'sports betting', 'wager': wager, 'win_loss': win_loss, 'win_loss_amount': float(payout)}

# COMMAND ----------

num_customers = 100
df = []

for p in range(num_customers):

### CONSTRUCT PLAYER
  customer_id = get_customer_id()
  customer_segment = get_customer_segment()
  age_band = get_age_band()
  gender = get_gender()
  registration_date = get_registration_date()
  last_active_date = get_last_active_date(customer_segment)
  flagged_high_risk = get_high_risk_flag(customer_segment)
  balance = round(float(0),2)

### GENERATE REGISTRATION BEACON AND APPEND TO DF ###
  df.append(get_registration_beacon(customer_id, registration_date, age_band, gender))
  
### GENERATE FLAGGED HIGH RISK BEACON AND APPEND TO DF ###
  
  if flagged_high_risk == 1:
    flagged_high_risk_beacon = get_flagged_high_risk_beacon(customer_id, last_active_date)
    df.append(flagged_high_risk_beacon)

### PLACE BETS  
  list_of_active_days = get_list_of_active_days(customer_segment, registration_date, last_active_date)
  for date in list_of_active_days:
    date = date
    date_transaction_id = 0
    
    # Instantiate new tables each active day (Baccarat, Blackjack, Craps, Roulette, Sports Betting, Slots, Video Poker)
    b = Blackjack()
    r = Roulette()
    s = Slots()
    v = VideoPoker()
    ba = Baccarat()
    c = Craps()
    sb = SportsBetting()
    game_dict = {'Baccarat': ba,'Blackjack': b, 'Craps': c, 'Roulette': r, 'Slots': s, 'Sports Betting': sb, 'Video Poker': v}
    
    # Get list of game type bets for the day
    bets_list = get_daily_bets_by_game_type(customer_segment)
    
    for game in bets_list:

      if game in game_dict.keys():
        g = game_dict[game] # grabs the game object instantiated above
        wager = get_bet_wager(customer_segment)
        
        # Make sure player has $ to bet
        while wager > balance:
          date_transaction_id += 1
          initial_balance = round(balance,2)
          deposit_amount = get_deposit_amount(customer_segment)
          balance += deposit_amount
          # GENERATE DEPOSIT BEACON AND APPEND TO DF
          df.append(get_deposit_beacon(customer_id, date,date_transaction_id, deposit_amount, initial_balance, balance))
        
        # Play the game
        date_transaction_id += 1
        initial_balance = round(balance,2)
        outcome = g.play(wager)
        balance += outcome['win_loss_amount']
        ending_balance = round(balance,2)
    
    # GENERATE BET BEACON AND APPEND TO DF
        bet_beacon = get_bet_beacon(
          customer_id,
          date,
          date_transaction_id,
          outcome['game_type'],
          outcome['wager'],
          outcome['win_loss'],
          outcome['win_loss_amount'],
          initial_balance,
          ending_balance)
        df.append(bet_beacon)
      
      if game == 'done':
        is_withdrawal = get_is_withdrawal_day(customer_segment)
        if is_withdrawal & (balance > 5):
          date_transaction_id += 1
          initial_balance = round(balance,2)
          withdrawal_amount = round(-balance,2)
          balance += withdrawal_amount
          df.append(get_withdrawal_beacon(customer_id, date, date_transaction_id,withdrawal_amount, initial_balance, balance))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Dataframe

# COMMAND ----------

schema = 'customer_id STRING, age_band STRING, gender STRING, date STRING, date_transaction_id INT, event_type STRING, game_type STRING, wager_amount FLOAT, win_loss STRING, win_loss_amount FLOAT, initial_balance FLOAT, ending_balance FLOAT, withdrawal_amount FLOAT, deposit_amount FLOAT'

# COMMAND ----------

df = spark.createDataFrame(df,schema=schema).sort('customer_id','date','date_transaction_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Display Dataframe

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write data out to storage

# COMMAND ----------

#dbutils.fs.rm('dbfs:/databricks_solacc/real_money_gaming/data/raw', True)

# COMMAND ----------

#dbutils.fs.mkdirs('dbfs:/databricks_solacc/real_money_gaming/data/raw')

# COMMAND ----------

df.write.csv('dbfs:/databricks_solacc/real_money_gaming/data/raw/rmg_data_{}.csv'.format(fake.uuid4))

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

display(spark.read.csv('dbfs:/databricks_solacc/real_money_gaming/data/raw/rmg_data_*.csv',schema=schema).filter(col('event_type') == 'flagged_high_risk'))

# COMMAND ----------


