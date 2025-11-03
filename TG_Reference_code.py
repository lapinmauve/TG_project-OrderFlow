'''
### Major UPdates
250107      MODIFY THE TELEGRAM SEND PROCESS TO GET AGGREGATED FLUSTUATION MESSAGE IF ABOVE TRESHOLD
250203      ADD A WARNING PROCESS WHEN MANY WATCHED TICKER ARE MOVING IN THE SAME DIRECTION
250206      ADD THE COMPLETE STOCKS DETAILS WHEN PRICE MOVMENT ALARM IS TRIGGERED
250211      ADD MINIMUM PRICE VARIATION TO TRIGGER MOVMENT ALARM (sa a commo  low movment do not trigger alarm....)
250904      modify streaming_STK_OPT_TRADE from 4 cols to 6 ---> added bid_size and ask_size as col 5 and 6

'''




# Below are the import statements

from ibapi.wrapper import *
from ibapi.client import *
from ibapi.contract import *
from ibapi.order import *
from threading import Thread
import queue
import datetime
import calendar
import time
import pandas as pd

import numpy as np
import os
import pytz
# import shutil
import requests

# import yfinance as yf
import sys

# import json
from dotenv import load_dotenv
# import io

from loguru import logger

from add_contract_option_0dte import add_0dte_option_contracts


logger.remove()
# logger.add("TG_main_app.log", rotation="1024 KB")
# logger.add("TG_main_app.log", rotation="1024 KB", level="DEBUG", backtrace=True, diagnose=True)
# logger.add("/home/manu/Documents/2009___GENESIS/IB/TG/TG_240508_base/TG_main_app.log", rotation="1024 KB", level="DEBUG", backtrace=True, diagnose=True)
logger.add("/Users/manu/Documents/code/TG_project/TG_base/TG_main_app.log", rotation="1024 KB", level="DEBUG", backtrace=True, diagnose=True)
logger.add(sys.stdout, colorize=True, format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

# logger.add(sys.stderr, level="ERROR") # Add a sink for console errors
logger.add(sys.stderr, format="{time:MMMM D, YYYY > HH:mm:ss} | {level} | {message} | {extra}")

load_dotenv()



# Below are the global variables
NYSE_isOpen = False
availableFunds = 0
buyingPower = 0
CashBalance_CAD = 0.0
CashBalance_USD = 0.0
Account_NetLiquidation_CAD = 0.0
positionsDict = {}

# Option data
OptionData = []

# Below are the custom classes and methods



# Below are the custom classes and methods


def contractCreate(symbolEntered):
    # Standard contract
    contract1 = Contract()  # Creates a contract object from the import
    contract1.symbol = symbolEntered   # Sets the ticker symbol
    contract1.secType = "STK"   # Defines the security type as stock
    contract1.currency = "USD"  # Currency is US dollars
    # In the API side, NASDAQ is always defined as ISLAND in the exchange field
    contract1.exchange = "SMART"
    # contract1.PrimaryExch = "NYSE"


    # Specific ticker have differents exchange settings... as below
    if symbolEntered == 'SOXS' or symbolEntered == 'SOXL' or symbolEntered == 'SPXS' or symbolEntered == 'SPXL':
        contract1.secType = "STK"   # Defines the security type as stock
        contract1.currency = "USD"  # Currency is US dollars
        # In the API side, NASDAQ is always defined as ISLAND in the exchange field
        contract1.exchange = "ARCA"

    # if symbolEntered == 'SOXS':
    #     contract1.secType = "STK"   # Defines the security type as stock
    #     contract1.currency = "USD"  # Currency is US dollars
    #     # In the API side, NASDAQ is always defined as ISLAND in the exchange field
    #     contract1.exchange = "ARCA"

    if symbolEntered == 'META':
        contract1.secType = "STK"   # Defines the security type as stock
        contract1.currency = "USD"  # Currency is US dollars
        # In the API side, NASDAQ is always defined as ISLAND in the exchange field
        contract1.exchange = "ISLAND"

    if symbolEntered == 'VIX' or symbolEntered == 'VIX9D' or symbolEntered == 'VIX3M':
        contract1.secType = "FUT"   # Defines the security type as stock
        contract1.currency = "USD"  # Currency is US dollars
        contract1.exchange = "CBOE"
        # contract1.PrimaryExch = "CBOE"

    return contract1    # Returns the contract object

# def contractCreate_ARCA(symbolEntered):
#     # Fills out the contract object
#     contract1 = Contract()  # Creates a contract object from the import
#     contract1.symbol = symbolEntered   # Sets the ticker symbol
#     contract1.secType = "STK"   # Defines the security type as stock
#     contract1.currency = "USD"  # Currency is US dollars
#     # In the API side, NASDAQ is always defined as ISLAND in the exchange field
#     contract1.exchange = "ARCA"
#     # contract1.PrimaryExch = "ARCA"
#     return contract1    # Returns the contract object

# def contractCreate_NASDAQ(symbolEntered):
#     # Fills out the contract object
#     contract1 = Contract()  # Creates a contract object from the import
#     contract1.symbol = symbolEntered   # Sets the ticker symbol
#     contract1.secType = "STK"   # Defines the security type as stock
#     contract1.currency = "USD"  # Currency is US dollars
#     # In the API side, NASDAQ is always defined as ISLAND in the exchange field
#     contract1.exchange = "ISLAND"
#     return contract1    # Returns the contract object

def contract_Option(symbolEntered):
    # Fills out the contract object
    contract1 = Contract()  # Creates a contract object from the import
    contract1.symbol = symbolEntered   # Sets the ticker symbol
    contract1.secType = "OPT"   # Defines the security type as stock
    contract1.currency = "USD"  # Currency is US dollars
    # In the API side, NASDAQ is always defined as ISLAND in the exchange field
    contract1.exchange = "SMART"
    # contract1.PrimaryExch = "NYSE"
    return contract1    # Returns the contract object


def contract_Option_PUT(symbolEntered):
    # Fills out the contract object
    contract1 = Contract()  # Creates a contract object from the import
    contract1.symbol = symbolEntered   # Sets the ticker symbol
    contract1.secType = "OPT"   # Defines the security type as stock
    contract1.right = 'P'       # request PUT infos
    contract1.currency = "USD"  # Currency is US dollars
#    contract1.lastTradeDateOrContractMonth = '202011'
    # In the API side, NASDAQ is always defined as ISLAND in the exchange field
    contract1.exchange = "SMART"
    # contract1.PrimaryExch = "NYSE"
    return contract1    # Returns the contract object

#
def contract_Option_PUT_search(symbolEntered, Exercice_YYYMM):
    # Fills out the contract object
    print(Exercice_YYYMM)
    contract1 = Contract()  # Creates a contract object from the import
    contract1.symbol = symbolEntered   # Sets the ticker symbol
    contract1.secType = "OPT"   # Defines the security type as stock
    contract1.lastTradeDateOrContractMonth = Exercice_YYYMM
    contract1.right = 'P'       # request PUT infos
    contract1.currency = "USD"  # Currency is US dollars
    # In the API side, NASDAQ is always defined as ISLAND in the exchange field
    contract1.exchange = "SMART"
    # contract1.PrimaryExch = "NYSE"
    return contract1    # Returns the contract object

def contract_Option_PUT_TSE_search(symbolEntered, Exercice_YYYMM):
    # Fills out the contract object
    contract1 = Contract()  # Creates a contract object from the import
    contract1.symbol = symbolEntered   # Sets the ticker symbol
    contract1.secType = "OPT"   # Defines the security type as stock
    contract1.lastTradeDateOrContractMonth = Exercice_YYYMM
    contract1.right = 'P'       # request PUT infos
    contract1.currency = "CAD"  # Currency is US dollars
    # In the API side, NASDAQ is always defined as ISLAND in the exchange field
    contract1.exchange = "SMART"
#    contract1.PrimaryExch = "TSE"
    return contract1    # Returns the contract object

def contract_Option_PUT_Full(symbolEntered, strikePrice, ExerciceDate, optMultiplier):
    # Fills out the contract object
    contract1 = Contract()  # Creates a contract object from the import
    contract1.symbol = symbolEntered   # Sets the ticker symbol
    contract1.secType = 'OPT'   # Defines the security type as stock
    contract1.right = 'P'       # request PUT infos
    contract1.lastTradeDateOrContractMonth = ExerciceDate  # ex: '20201002'
    contract1.strike = strikePrice
    contract1.multiplier = optMultiplier
    contract1.currency = 'USD'  # Currency is US dollars
    # In the API side, NASDAQ is always defined as ISLAND in the exchange field
    contract1.exchange = 'SMART'
    # contract1.PrimaryExch = "NYSE"
    return contract1    # Returns the contract object

def orderCreate(quantityEntered=10):    # Defaults the quanity to 10 but is overriden later in orderExecution
    # Fills out the order object
    order1 = Order()    # Creates an order object from the import

    # If the quantity is positive then we want to buy at that quantity, and sell if it is negative
    if quantityEntered > 0:
        order1.action = "BUY"   # Sets the order action to buy
        order1.totalQuantity = int(quantityEntered)   # Uses the quantity passed in orderExecution
    else:
        order1.action = "SELL"
        order1.totalQuantity = abs(int(quantityEntered))   # Uses the quantity passed in orderExecution

    order1.orderType = "MKT"    # Sets order type to market buy
    order1.transmit = True
    return order1   # Returns the order object

def orderCreate_PEG_MID(quantityEntered, offset, limitPrice):    # Defaults the quanity to 10 but is overriden later in orderExecution
    # Fills out the order object
    order1 = Order()    # Creates an order object from the import

    # If the quantity is positive then we want to buy at that quantity, and sell if it is negative
    if quantityEntered > 0:
        order1.action = "BUY"   # Sets the order action to buy
        order1.totalQuantity = int(quantityEntered)   # Uses the quantity passed in orderExecution
    else:
        order1.action = "SELL"
        order1.totalQuantity = abs(int(quantityEntered))   # Uses the quantity passed in orderExecution

    order1.orderType = "PEG MID"    # Sets order type to market buy
    order1.auxPrice = offset
    order1.lmtPrice = limitPrice
    order1.transmit = True
    return order1   # Returns the order object



def orderExecution(symbolEntered, _qty):
    #Places the order with the returned contract and order objects

    # Call client methods to gather most recent information
    contractObject = contractCreate(symbolEntered)
    orderObject = orderCreate(_qty)
#    app.price_update(contractObject, app.nextOrderId())
    nextID = app.nextOrderId()

    time.sleep(2)   # Waits for the price_update request to finish. May be increased for slower connections
#    print("Global Tick Price: " + str(stockPrice))

    # Print statement to confirm correct values
    print("The next valid id is - " + str(nextID))
#    print("Buying power " + str(buyingPower))
#    print("Available Funds " + str(availableFunds))

    # Place order
    app.placeOrder(nextID, contractObject, orderObject)
    print("order was placed")

def orderExecution_PEG_MID(symbolEntered, _qty, _offset, _limitPrice):
    #Places the order with the returned contract and order objects

    # Call client methods to gather most recent information
    contractObject = contractCreate(symbolEntered)
    orderObject = orderCreate_PEG_MID(_qty, _offset, _limitPrice)
#    app.price_update(contractObject, app.nextOrderId())
    nextID = app.nextOrderId()

    time.sleep(2)   # Waits for the price_update request to finish. May be increased for slower connections
#    print("Global Tick Price: " + str(stockPrice))

    # Print statement to confirm correct values
    print("The next valid id is - " + str(nextID))
#    print("Buying power " + str(buyingPower))
#    print("Available Funds " + str(availableFunds))

    # Place order
    app.placeOrder(nextID, contractObject, orderObject)
    print("order was placed")

# Below is the TestWrapper/EWrapper class

'''Here we will override the methods found inside api files'''

class TestWrapper(EWrapper):

    # error handling methods
    def init_error(self):
        error_queue = queue.Queue()
        self.my_errors_queue = error_queue

    def is_error(self):
        error_exist = not self.my_errors_queue.empty()
        return error_exist

    def get_error(self, timeout=6):
        if self.is_error():
            try:
                return self.my_errors_queue.get(timeout=timeout)
            except queue.Empty:
                return None
        return None

    def error(self, id, errorCode, errorString):
        ## Overrides the native method
        errormessage = "IB returns an error with %d errorcode %d that says %s" % (id, errorCode, errorString)
        self.my_errors_queue.put(errormessage)

    # time handling methods
    def init_time(self):
        time_queue = queue.Queue()
        self.my_time_queue = time_queue
        return time_queue

    def currentTime(self, server_time):
        ## Overriden method
        self.my_time_queue.put(server_time)

    # ID handling methods
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    # Account details handling methods
    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
#        print("Acct Summary. ReqId:", reqId, "Acct:", account, "Tag: ",
#            tag, "Value:", value, "Currency:", currency)
        if tag == 'CashBalance' and currency == 'CAD':
            global CashBalance_CAD
            CashBalance_CAD = value
        if tag == 'CashBalance' and currency == 'USD':
            global CashBalance_USD
            CashBalance_USD = value

        if tag == 'NetLiquidationByCurrency' and currency == 'BASE':
            global Account_NetLiquidation_CAD
            Account_NetLiquidation_CAD = value

        if tag == "AvailableFunds":
            global availableFunds
            availableFunds = value

        if tag == "BuyingPower":
            global buyingPower
            buyingPower = value

    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        print("AccountSummaryEnd. Req Id: ", reqId)

    # Position handling methods
    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        super().position(account, contract, position, avgCost)
        positionsDict[contract.symbol] = {'positions' : position, 'avgCost' : avgCost}
        print("Position.", account, "Symbol:", contract.symbol, "SecType:", contract.secType, "Currency:", contract.currency,"Position:", position, "Avg cost:", avgCost)

    # Market Price handling methods
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
        # print("Tick Price. Ticker Id:", reqId, "tickType:",
            # tickType, "Price:", price, "CanAutoExecute:", attrib.canAutoExecute,
            # "PastLimit:", attrib.pastLimit, end=' ')

        global streaming_STK_OPT_TRADE  # Declares that we want stockPrice to be treated as a global variable
        global streaming_100_OPT
        global ATR_STK_vect
        global streaming_STK_OPT_currentPrices_vect
        # global bid_price_list
        # global ask_price_list


        # Fill streaming_STK_OPT_TRADE ----> "STOCK section" global array
        if reqId < streaming_STK_nb:
            # Use tickType 1 for bid Price (col #1)
            if tickType == 1:
                streaming_STK_OPT_TRADE[reqId,1] = price
                # bid_price_list.append(streaming_STK_OPT_TRADE[:,1])

            # Use tickType 2 for ask Price (col #2)
            if tickType == 2:
                streaming_STK_OPT_TRADE[reqId,2] = price
                # ask_price_list.append(streaming_STK_OPT_TRADE[:,2])


            # Use tickType 4 (Last Price) if you are running during the market day (col #0)
            if tickType == 4:

    #            print('\nNew Tick Price for '+stock_symbols_list[int(reqId)]+' @ ' + str(price))
                streaming_STK_OPT_TRADE[reqId,0] = price

            # Uses tickType 9 (Close Price) if after market hours  (col #0)
            elif tickType == 9:
    #            print('\nNew Tick Price for '+stock_symbols_list[int(reqId)]+' @ ' + str(price))

    #            stockPrice = price
                streaming_STK_OPT_TRADE[reqId,0] = price

        # Fill streaming_STK_OPT_TRADE ----> "OPTION section" global array
        if reqId >= streaming_STK_nb: # and reqId < streaming_TRADE_first_index:
            # Use tickType 1 for bid Price (col #1)
            if tickType == 1:
                streaming_STK_OPT_TRADE[reqId,1] = price

            # Use tickType 2 for ask Price (col #2)
            if tickType == 2:
                streaming_STK_OPT_TRADE[reqId,2] = price

            # Update Option SENS ratio when bid or ask are updated
            # streaming_STK_OPT_TRADE[reqId,3] = (    (streaming_STK_OPT_TRADE[reqId,0] * 0.03) / ((streaming_STK_OPT_TRADE[reqId,1] + streaming_STK_OPT_TRADE[reqId,2] / 2.0 ) + 0.001   )    ) * 100.0
            # streaming_STK_OPT_TRADE[reqId,3] = (    ATR_STK_vect[reqId,0]/ ((streaming_STK_OPT_TRADE[reqId,1] + streaming_STK_OPT_TRADE[reqId,2] / 2.0 ) + 0.001   )    ) * 100.0
            # streaming_STK_OPT_TRADE[reqId,3] = (    ATR_STK_vect[reqId,0]/ (((streaming_STK_OPT_TRADE[reqId,1] + streaming_STK_OPT_TRADE[reqId,2]) / 2.0 ) + 0.001   )    ) * 100.0
            # streaming_STK_OPT_TRADE[reqId,3] = (    ATR_STK_vect[reqId,0]/ (((streaming_STK_OPT_TRADE[reqId,1] + streaming_STK_OPT_TRADE[reqId,2]) / 2.0 ) - (streaming_STK_OPT_TRADE[reqId,0] - streaming_STK_OPT_currentPrices_vect[reqId,0] )   )    ) * 100.0





    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        super().tickSize(reqId, tickType, size)
#        print("Tick Size. Ticker Id:", reqId, "tickType:", tickType, "Size:", size)

        global streaming_STK_OPT_TRADE  # Declares that we want stockPrice to be treated as a global variable
        # global bid_size_list
        # global ask_size_list
        # global data_2save_STK_list_idx


        # Fill STK global array
        if reqId < streaming_STK_nb:
            # Use tickType 8 for delta volume : previous - current incrasing v0lume data (col #3)
            if tickType == 8:
                streaming_STK_OPT_TRADE[reqId,3] = size

            # Use tickType 0 for bid Size (col #5)
            if tickType == 0:
                # streaming_STK_OPT_TRADE[reqId,4] = size
                # streaming_STK_OPT_TRADE[reqId,4] = streaming_STK_OPT_TRADE[reqId,4]  + int(size)
                streaming_STK_OPT_TRADE[reqId,4] += int(size)
                # temp_array = np.zeros((len(data_2save_STK_list_idx),1))
                # temp_array[reqId,0] = size
                # bid_size_list.append(temp_array)


            # Use tickType 3 for ask Size (col #6)
            if tickType == 3:
                # streaming_STK_OPT_TRADE[reqId,5] = size
                # streaming_STK_OPT_TRADE[reqId,5] = streaming_STK_OPT_TRADE[reqId,5] + int(size)
                streaming_STK_OPT_TRADE[reqId,5] += int(size)
                # temp_array = np.zeros((len(data_2save_STK_list_idx),1))
                # temp_array[reqId,0] = size
                # ask_size_list.append(temp_array)


#        # Fill OPTION global array
#        if reqId >= streaming_STK_nb and reqId < streaming_TRADE_first_index:
#            if tickType == 8:
#                streaming_STK_OPT_TRADE[reqId-100,3] = size

    def tickString(self, reqId: TickerId, tickType: TickType, value: str):
        super().tickString(reqId, tickType, value)
#        print("Tick string. Ticker Id:", reqId, "Typxe:", tickType, "Value:", value)

    def tickGeneric(self, reqId: TickerId, tickType: TickType, value: float):
        super().tickGeneric(reqId, tickType, value)
#        print("Tick Generic. Ticker Id:", reqId, "tickType:", tickType, "Value:", value)

#    def contractDetails(self, reqId, contractDetails):
#        global OptionData
#        OptionData = contractDetails
#        print('contractDetails: ', reqId, ' ',contractDetails, '\n' )

    def contractDetails(self, reqId, contractDetails):
#        OptionData = contractDetails.contract
        OptionData.append(contractDetails.contract)

#        print(reqId, contractDetails.contract)# my version doesnt use summary



# Below is the TestClient/EClient Class

'''Here we will call our own methods, not overriding the api methods'''

class TestClient(EClient):

    def __init__(self, wrapper):
    ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)

    def server_clock(self):

        print("Asking server for Unix time")

        # Creates a queue to store the time
        time_storage = self.wrapper.init_time()

        # Sets up a request for unix time from the Eclient
        self.reqCurrentTime()

        #Specifies a max wait time if there is no connection
        max_wait_time = 10

        try:
            requested_time = time_storage.get(timeout = max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached")
            requested_time = None

        while self.wrapper.is_error():
          print("Error:")
          print(self.get_error(timeout=5))

        return requested_time

    def account_update(self):
#        self.reqAccountSummary(9001, "All", "TotalCashValue, BuyingPower, AvailableFunds")
#        self.reqAccountSummary(9001, "All", "CashBalance, BuyingPower, AvailableFunds, NetLiquidationByCurrency")
#        self.reqAccountSummary(9001, "All", AccountSummaryTags.AllTags)
        self.reqAccountSummary(9004, "All", "$LEDGER:ALL")





    def position_update(self):
        self.reqPositions()

    def price_update(self, Contract, tickerid):
        self.reqMktData(tickerid, Contract, "", False, False, [])
        return tickerid


# Below is TestApp Class

class TestApp(TestWrapper, TestClient):
    #Intializes our main classes
    def __init__(self, ipaddress, portid, clientid):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)

        #Connects to the server with the ipaddress, portid, and clientId specified in the program execution area
        self.connect(ipaddress, portid, clientid)

        #Initializes the threading
        thread = Thread(target = self.run)
        thread.start()
        setattr(self, "_thread", thread)

        #Starts listening for errors
        self.init_error()


# Below local functions declaration

def fcn_OptionContract_get_DateStrikes(Option_symbol, Stock_currentPrice, OptionExpirationDelay_days, _reqId):
    """
    #  ---------------- Automated search option process ----------------
    # Input var expl:
    OptionExpirationDelay_days = 15
    Option_symbol = 'AAPL'
    Stock_currentPrice = 119.27
    """
    global OptionData
    # automated section
    OptionData = [] # clear data before contract search begin...

    # Get current YYYY and MM
    today = str(datetime.date.today())
    curr_year = int(today[:4])
    curr_month = int(today[5:7])
    curr_day = int(today[8:10])

    # check if price is != 0.0
    if Stock_currentPrice == 0.0:
        print('EXIT A /// Error: Strike price is 0.0 !!!')
        return [0,0,0,0]


    ### --------------------------------------------------------------------------- NORMAL SEARCH -------------------------
    # Check if the desired delay in nb days goes into next month
    if curr_day + OptionExpirationDelay_days >= 30:
        curr_month += 1

        if curr_month<= 9:
            current_YYYMM_str = str(curr_year) + '0'+str(curr_month)
        else:
            current_YYYMM_str = str(curr_year) + str(curr_month)

        # correct for december when it skip to next year...
        if curr_month == 13:
            curr_month = 1
            curr_year += 1

            if curr_month<= 9:
                current_YYYMM_str = str(curr_year) + '0'+str(curr_month)
            else:
                current_YYYMM_str = str(curr_year) + str(curr_month)
    else:
        current_YYYMM_str = str(curr_year) + str(curr_month)


    # current_YYYMM_str = str(curr_year) + str(curr_month)
    print(current_YYYMM_str)
    contractOption_search = contract_Option_PUT_search(Option_symbol, current_YYYMM_str)
    app.reqContractDetails(_reqId, contractOption_search)  # ca marche... et n<ajoute pas un outil instrument dans la qte de 100 max simultanes
    time.sleep(2)
    if len(OptionData) == 0:
        time.sleep(10)   # ---------------------- Manage error if there is no datareturned by the get contract search!!! .. skipp this trial!!!

    if  len(OptionData) == 0:
        print('EXIT B /// Error... no contract found for: '+Option_symbol+' ... OptionData is EMPTY')
        print('>>>>>------ current_YYYMM_str: '+current_YYYMM_str)
        return [0,0,0,0]
    # parse OptionData to construct array
    # array cols : [conId, dateContractExp, multiplier, strikePrice ]
    OptionChain_infos = np.zeros((len(OptionData), 4))

    for i in range(0,len(OptionChain_infos)):
        # Fill array
        OptionChain_infos[i,0] = OptionData[i].conId
        OptionChain_infos[i,1] = OptionData[i].lastTradeDateOrContractMonth
        OptionChain_infos[i,2] = OptionData[i].strike
        OptionChain_infos[i,3] = OptionData[i].multiplier


    # store data in a df easy to sort
    OptionChain_infos_df = pd.DataFrame()
    OptionChain_infos_df['conId'] = OptionChain_infos[:,0].astype(int)
    OptionChain_infos_df['lastTradeDateOrContractMonth'] = OptionChain_infos[:,1].astype(int)
    OptionChain_infos_df['strike'] = OptionChain_infos[:,2]
    OptionChain_infos_df['multiplier'] = OptionChain_infos[:,3].astype(int)

    # sort by contract date
    #OptionChain_infos_df = OptionChain_infos_df.sort_values(by='lastTradeDateOrContractMonth', ascending=True)
    OptionChain_infos_df = OptionChain_infos_df.sort_values(by=['lastTradeDateOrContractMonth', 'strike'], ascending=[True, True])
    OptionChain_infos_df = OptionChain_infos_df.reset_index()

    DataOption_list_uniq = list(OptionChain_infos_df['lastTradeDateOrContractMonth'].unique())
#    print(' ')
#    print(' ---------------->>> DataOption_list_uniq  NORMAL SEARCH <<<<----------------------') # debugg..
#
#    print(DataOption_list_uniq) # debugg..
#    print(' ')


    # get the contract date  required by user specification (OptionExpirationDelay_days)
    Date_today = datetime.datetime.strptime(today, '%Y-%m-%d')

    flag_found_MATCH = 0
    for i in range(0,len(DataOption_list_uniq)):

        # convert string date in datetime object
        contractDate = datetime.datetime.strptime(str(DataOption_list_uniq[i]), '%Y%m%d')

        nb_days_until_expiration = contractDate - Date_today
        if nb_days_until_expiration.days >= OptionExpirationDelay_days:
            desired_dateContract = str(DataOption_list_uniq[i])
            print('Found '+Option_symbol+' Option Contract date '+desired_dateContract+' with expiration in: ' + str(nb_days_until_expiration.days) + ' days')
            flag_found_MATCH = 1
            break


    ### --------------------------------------------------------------- END OF NORMAL SEARCH ----------------------------



    ### --------------------------------------------------------------------------- SEARCH NEXT MONTH if no match found... -------------------------
    if flag_found_MATCH == 0 :
    # Check if the desired delay in nb days goes into next month
        curr_month += 1 # add a month!!!

        if curr_month<= 9:
            current_YYYMM_str = str(curr_year) + '0'+str(curr_month)
        else:
            current_YYYMM_str = str(curr_year) + str(curr_month)

        # correct for december when it skip to next year...
        if curr_month == 13:
            curr_month = 1
            curr_year += 1

            if curr_month<= 9:
                current_YYYMM_str = str(curr_year) + '0'+str(curr_month)
            else:
                current_YYYMM_str = str(curr_year) + str(curr_month)

        # current_YYYMM_str = str(curr_year) + str(curr_month)
        # print(current_YYYMM_str)
        contractOption_search = contract_Option_PUT_search(Option_symbol, current_YYYMM_str)
        app.reqContractDetails(_reqId, contractOption_search)  # ca marche... et n<ajoute pas un outil instrument dans la qte de 100 max simultanes
        time.sleep(5)
        if len(OptionData) == 0:
            time.sleep(10)   # ---------------------- Manage error if there is no datareturned by the get contract search!!! .. skipp this trial!!!

        if  len(OptionData) == 0:
            print('Error... no contract found... retry')
        # parse OptionData to construct array
        # array cols : [conId, dateContractExp, multiplier, strikePrice ]
        OptionChain_infos = np.zeros((len(OptionData), 4))

        for i in range(0,len(OptionChain_infos)):
            # Fill array
            OptionChain_infos[i,0] = OptionData[i].conId
            OptionChain_infos[i,1] = OptionData[i].lastTradeDateOrContractMonth
            OptionChain_infos[i,2] = OptionData[i].strike
            OptionChain_infos[i,3] = OptionData[i].multiplier


        # store data in a df easy to sort
        OptionChain_infos_df = pd.DataFrame()
        OptionChain_infos_df['conId'] = OptionChain_infos[:,0].astype(int)
        OptionChain_infos_df['lastTradeDateOrContractMonth'] = OptionChain_infos[:,1].astype(int)
        OptionChain_infos_df['strike'] = OptionChain_infos[:,2]
        OptionChain_infos_df['multiplier'] = OptionChain_infos[:,3].astype(int)

        # sort by contract date
        #OptionChain_infos_df = OptionChain_infos_df.sort_values(by='lastTradeDateOrContractMonth', ascending=True)
        OptionChain_infos_df = OptionChain_infos_df.sort_values(by=['lastTradeDateOrContractMonth', 'strike'], ascending=[True, True])
        OptionChain_infos_df = OptionChain_infos_df.reset_index()

        DataOption_list_uniq = list(OptionChain_infos_df['lastTradeDateOrContractMonth'].unique())
#        print(' ')
#        print(' ---------------->>> DataOption_list_uniq SEARCH NEXT MONTH if no match found <<<<----------------------') # debugg..
#
#        print(DataOption_list_uniq) # debugg..
#        print(' ')
#
            # get the contract date  required by user specification (OptionExpirationDelay_days)
        Date_today = datetime.datetime.strptime(today, '%Y-%m-%d')

        for i in range(0,len(DataOption_list_uniq)):

            # convert string date in datetime object
            contractDate = datetime.datetime.strptime(str(DataOption_list_uniq[i]), '%Y%m%d')

            nb_days_until_expiration = contractDate - Date_today
            if nb_days_until_expiration.days >= OptionExpirationDelay_days:
                desired_dateContract = str(DataOption_list_uniq[i])
                print('Found '+Option_symbol+' Option Contract date '+desired_dateContract+' with expiration in: ' + str(nb_days_until_expiration.days) + ' days')
                flag_found_MATCH = 1
                break


    ### --------------------------------------------------------------- END OF SEARCH NEXT MONTH if no match found...  ----------------------------

    if flag_found_MATCH == 0: # still no match found...
        print('EXIT C /// No contract found!!! return 0 in the list with 4 items, debugg infos are following:')
        print(' ')
        print(' ---------------->>> DataOption_list_uniq <<<<----------------------') # debugg..
        print(DataOption_list_uniq) # debugg..
        print(' ')
        return [0,0,0,0]

    # Retreive only this contract date from df
    OptionChain_selectDate_df = OptionChain_infos_df[OptionChain_infos_df['lastTradeDateOrContractMonth'] == int(desired_dateContract)]
    OptionChain_selectDate_df = OptionChain_selectDate_df.reset_index()

#    print(OptionChain_selectDate_df)

    try:
        # get the  OUT-the money / AT-the money / IN-the Money strikes prices
        for i in range(0,len(OptionChain_selectDate_df)):

            if OptionChain_selectDate_df['strike'][i] >= Stock_currentPrice :
                Strike_Out_At_In_Money = [  OptionChain_selectDate_df['strike'][i-1] , OptionChain_selectDate_df['strike'][i] , OptionChain_selectDate_df['strike'][i+1]   ]   # ------------------------------------------- Manage error if i-1 or i+1 are outside index boundary
                print(Strike_Out_At_In_Money)
                break
        if i == len(OptionChain_selectDate_df)-1 :
            # we did'n found a Strike_Out_At_In_Money so we exit in error mode...
            print('EXIT D /// Strike_Out_At_In_Money seeking, debugg infos are following:')
            print('Stock current price:', Stock_currentPrice)
            print(' ---------------->>> DataOption_list_uniq <<<<----------------------') # debugg..
            print(DataOption_list_uniq) # debugg..
            print(' ---------------->>> OptionChain_selectDate_df <<<<----------------------') # debugg..
            print(OptionChain_selectDate_df) # debugg..
            print(' ')
            return [0,0,0,0]



    except:
        print('EXIT E /// Problem with index [i+1], [i-1], debugg infos are following:')
        print('Stock current price:', Stock_currentPrice)
        print(' ---------------->>> DataOption_list_uniq <<<<----------------------') # debugg..
        print(DataOption_list_uniq) # debugg..
        print(' ---------------->>> OptionChain_selectDate_df <<<<----------------------') # debugg..
        print(OptionChain_selectDate_df) # debugg..
        print(' ')
        return [0,0,0,0]

    # Add contract date at the end of list
    Strike_Out_At_In_Money.append(desired_dateContract)

    return Strike_Out_At_In_Money

def fcn_DataStreaming_start_STK_single(_stock, _reqId):
    global streaming_instrument_metadata
    contractObject = contractCreate(_stock)
    # app.cancelMktData(_reqId) # reset instrument before assigmnent
    # time.sleep(1.5)   # Waits for the price_update request to finish. May be increased for slower connections
    app.price_update(contractObject, _reqId)
    streaming_instrument_metadata[_reqId] = {
        "type": "STK",
        "symbol": _stock,
        "reqId": _reqId,
        "contract": contractObject,
    }
    time.sleep(0.5)   # Waits for the price_update request to finish. May be increased for slower connections
    print('(reqId: '+str(_reqId)+')   Start data streaming for symbol: '+_stock)
    return

def fcn_DataStreaming_start_STK_All(): # start streamin ALL stocl from stock_symbols_list
    global streaming_STK_nb
    global stock_symbols_list
    global streaming_instrument_metadata
    # print(stock_symbols_list)

    #clearing old data to avoid overlappingof old pries on new ticker if prices do not fluctuate in the update period...
    streaming_STK_OPT_TRADE[:,0] = 0.0
    streaming_STK_OPT_TRADE[:,1] = 0.0
    streaming_STK_OPT_TRADE[:,2] = 0.0
    streaming_STK_OPT_TRADE[:,3] = 0.0
    streaming_STK_OPT_TRADE[:,4] = 0.0
    streaming_STK_OPT_TRADE[:,5] = 0.0
    streaming_instrument_metadata = [None] * len(streaming_instrument_metadata)

    # print('Cancel all current streaming slots (nb='+str(STK_bloc_load_nb)+')')
    # # Cancel all current streaming slots:
    # for i in range(0,STK_bloc_load_nb):
    #     app.cancelMktData(i)
    #     time.sleep(0.5)

    # print('Wait 5 sec. for cancel request to get processed...')
    # time.sleep(5.0) # wait for request to get processed...
    for i in range(0,streaming_STK_nb):
        # print(' --->>> '+stock_symbols_list[i])
        fcn_DataStreaming_start_STK_single(stock_symbols_list[i], i)
        time.sleep(2.5)


def fcn_DataStreaming_start_OPT(_stock, _OPT_exp_nbDays, _reqId):
    """
    _reqId: row index based onstreaming_STK_OPT_TRADE array

    """
    global streaming_STK_nb
    global streaming_STK_OPT_TRADE
    global streaming_instrument_metadata

    # Get the Option contract infos [strikes_OUT_money,strikes_AT_money,strikes_IN_money, contract_date]
    STK_price = streaming_STK_OPT_TRADE[_reqId-streaming_STK_nb,0]  # get the base stock actual price from streaming_STK_OPT_TRADE stock section
#    print('Stock: ',_stock,' - current price : ', STK_price)
        #get option underliying stock index in streaming_STK_OPT_TRADE array
    try:
        stock__index = stock_symbols_list.index(_stock)
        STK_price = streaming_STK_OPT_TRADE[stock__index,0]  # get the base stock actual price from streaming_STK_OPT_TRADE stock section

    except:
        print('Stock not in the main list... exit with error (return = 0)')
        return 0 # error flag

    print('Option parameters /// STK:',_stock,' STK_price: ', STK_price, ' _reqId: ',_reqId)
    optionContractInfos = fcn_OptionContract_get_DateStrikes(_stock, STK_price, _OPT_exp_nbDays, _reqId)

    if optionContractInfos[0] == 0:
        print('No contract found for '+_stock+' !!!  .......................... SKIP this option contract streaming!!!')
        return 0 # error flag

    # create full PUT contract
    OPT_Strike = optionContractInfos[1]   # for PUT: [0]= out of money, [1]= at the money, [2]= in the money,
    OPT_ContractDate = optionContractInfos[3]
    contractOption_FULL = contract_Option_PUT_Full(_stock, OPT_Strike, OPT_ContractDate, '100')
    print('PUT Option contract desc: ',contractOption_FULL)

    # clear instrument to reset
    app.cancelMktData(_reqId)
    time.sleep(2.0)
    app.reqMktData(_reqId, contractOption_FULL, '', False, False, [])

    # Fill strike price in streaming_100_OPT array (col# 0)
    streaming_STK_OPT_TRADE[_reqId,0] = OPT_Strike
    # Fill option_contractDate_list with option contract date
    option_contractDate_list[_reqId-streaming_STK_nb] = OPT_ContractDate

    streaming_instrument_metadata[_reqId] = {
        "type": "OPT",
        "symbol": _stock,
        "reqId": _reqId,
        "strike": OPT_Strike,
        "expiry": OPT_ContractDate,
        "right": "P",
        "contract": contractOption_FULL,
    }

    return 1

def fcn_DataStreaming_start_OPT_fullInfos(_stock, _strike, _contractDate, _reqId):
    """
    _reqId: row index based onstreaming_STK_OPT_TRADE array

    """
    global streaming_STK_nb
    global streaming_STK_OPT_TRADE
    global streaming_instrument_metadata

    # create full PUT contract
    contractOption_FULL = contract_Option_PUT_Full(_stock, _strike, _contractDate, '100')
    print('PUT Option contract FULL desc: ',contractOption_FULL)

    # clear instrument to reset
    app.cancelMktData(_reqId)
    time.sleep(2.0)
    app.reqMktData(_reqId, contractOption_FULL, '', False, False, [])

    # Fill strike price in streaming_100_OPT array (col# 0)
    streaming_STK_OPT_TRADE[_reqId,0] = _strike

    streaming_instrument_metadata[_reqId] = {
        "type": "OPT",
        "symbol": _stock,
        "reqId": _reqId,
        "strike": _strike,
        "expiry": _contractDate,
        "right": "P",
        "contract": contractOption_FULL,
    }

    return 1



def fcn_DataStreaming_start_OPT_SetStrike_All(): # start streamin ALL OPTIONS from the first (streaming_STK_nb -streaming_TRADE_first_index) stock_symbols_list
    global streaming_STK_nb
    global stock_symbols_list
    global opt_expDelay_nDays
#    global streaming_TRADE_first_index

#    for i in range(streaming_STK_nb,streaming_TRADE_first_index):
    for i in range(0,len(option_symbols_list)): #process all options symbols in list


        _stock = option_symbols_list[i][4:]  # Get underlying stock name ---> removing the 'OPT_' prefix...
#        #get option underliying stock index in streaming_STK_OPT_TRADE array
#        try:
#            stock__index = stock_symbols_list.index(_stock)
#        except:
#            print('Stock not in the main list... exit with error (return = 0)')
#            return 0 # error flag

        fcn_DataStreaming_start_OPT(_stock, opt_expDelay_nDays, i+streaming_STK_nb)

def fcn_DataStreaming_start_OPT_update_All(): # start streamin ALL OPTIONS from the first (streaming_STK_nb -streaming_TRADE_first_index) stock_symbols_list
    global streaming_STK_nb
    global stock_symbols_list
    global opt_expDelay_nDays
    global GEN_OptionInfos_df_exist
#    global streaming_TRADE_first_index

    if GEN_OptionInfos_df_exist == False: # if there is no saved option contract infos... do a normal streming process with a set strike search...
        fcn_DataStreaming_start_OPT_SetStrike_All()
        print('[warning] DataStreaming_start_OPT_update_All -----> GEN_OptionInfos_df_exist == False, DO A FULL STREAMING PROCESS WITH NEW STRIKES PRICES')

        return

    print('[ok] DataStreaming_start_OPT_update_All -----> GEN_OptionInfos_df_exist == TRUE, STREAMING PROCESS WITH SPECIFIC STRIKES PRICES')

    # GEN_OptionInfos_df_exist is True...
    global CurrentOptionInfos_df
    for i in range(0,len(option_symbols_list)): #process all options symbols in list


        _stock = option_symbols_list[i][4:]  # Get underlying stock name ---> removing the 'OPT_' prefix...
        #check if we have curent symbol in CurrentOptionInfos_df
        try:
            stock__index = list(CurrentOptionInfos_df['stockSymbol']).index('OPT_'+_stock)
            _strike = CurrentOptionInfos_df['price'][stock__index]
            _contractDate = CurrentOptionInfos_df['optionExpirationDate'][stock__index]
            # stockSymbol is in CurrentOptionInfos_df ---> do a price update with specific strike price
            print('[ok] option symbol found in CurrentOptionInfos_df -----> STREAMING with a specific Strike and contractDate on OPT_',_stock)
            fcn_DataStreaming_start_OPT_fullInfos(_stock, _strike, _contractDate, i+streaming_STK_nb)
            option_contractDate_list[stock__index] = _contractDate
        except:
            print('[warning] option symbol NOT found in CurrentOptionInfos_df -----> STREAMING with NEW Strike and NEW contractDate on OPT_',_stock)
            fcn_DataStreaming_start_OPT(_stock, opt_expDelay_nDays, i+streaming_STK_nb)
            return 0 # error flag


def fcn_DataStreaming_start_OPT_TRADE(_stock, _OPT_exp_nbDays, TRADE_slot_nb):
    """
    TRADE_slot_nb: the slot number (0,1,2,3,4...) used by the BUY/SELL TRADE process

    """
    global streaming_TRADE_first_index
    global streaming_STK_OPT_TRADE
    global stock_symbols_list
    global streaming_instrument_metadata

    #get stock index in streaming_STK_OPT_TRADE
    try:
        stock__index = stock_symbols_list.index(_stock)
    except:
        print('Stock not in the main list... exit with error (return = 0)')
        return 0 # error flag


    # Get the Option contract infos [strikes_OUT_money,strikes_AT_money,strikes_IN_money, contract_date]
    STK_price = streaming_STK_OPT_TRADE[stock__index,0]  # get the base stock actual price from streaming_STK_OPT_TRADE stock section
    optionContractInfos = fcn_OptionContract_get_DateStrikes(_stock, STK_price, _OPT_exp_nbDays, streaming_TRADE_first_index+TRADE_slot_nb)

    if optionContractInfos[0] == 0:
        print('No contract found!!!  .......................... SKIP this option contract streaming!!!')
        return 0 # error flag

    # create full PUT contract
    # OPT_Strike = optionContractInfos[0]
    OPT_Strike = optionContractInfos[1]   # for PUT: [0]= out of money, [1]= at the money, [2]= in the money,
    OPT_ContractDate = optionContractInfos[3]
    contractOption_FULL = contract_Option_PUT_Full(_stock, OPT_Strike, OPT_ContractDate, '100')
    print('PUT Option contract desc: ',contractOption_FULL)

    # clear instrument to reset
    _reqId = TRADE_slot_nb + streaming_TRADE_first_index
    app.cancelMktData(_reqId)
    time.sleep(2.0)
    app.reqMktData(_reqId, contractOption_FULL, '', False, False, [])

    # Fill strike price in streaming_100_OPT array (col# 0)
    streaming_STK_OPT_TRADE[_reqId,0] = OPT_Strike


    streaming_instrument_metadata[_reqId] = {
        "type": "OPT_TRADE",
        "symbol": _stock,
        "reqId": _reqId,
        "strike": OPT_Strike,
        "expiry": OPT_ContractDate,
        "right": "P",
        "contract": contractOption_FULL,
    }


    return [_stock, stock__index, OPT_Strike, OPT_ContractDate]



def fcn_DataStreaming_start_0DTE_options(option_metadata, generic_tick_list="106"):
    global app
    global streaming_instrument_metadata

    for entry in option_metadata:
        req_id = entry["reqId"]
        contract = entry["contract"]
        label = entry.get("label", "OPT")

        try:
            app.cancelMktData(req_id)
            time.sleep(0.5)
        except Exception:
            pass

        app.reqMktData(req_id, contract, generic_tick_list, False, False, [])
        streaming_instrument_metadata[req_id] = {
            "type": "OPT_0DTE",
            "symbol": contract.symbol,
            "reqId": req_id,
            "strike": entry.get("strike"),
            "expiry": entry.get("expiry"),
            "right": entry.get("right"),
            "contract": contract,
        }
        logger.info(
            "Started streaming for {} (reqId={}, strike={}, right={}, expiry={})",
            label,
            req_id,
            entry.get("strike"),
            entry.get("right"),
            entry.get("expiry"),
        )
        time.sleep(0.5)


def fcn_get_instrument_metadata_list():
    """Return a snapshot of instrument metadata for each streaming slot."""
    global streaming_instrument_metadata
    return list(streaming_instrument_metadata)

def fcn_DataStreaming_stop_OPT_TRADE(_stock, TRADE_slot_nb):
    """
    TRADE_slot_nb: the slot number (0,1,2,3,4...) used by the BUY/SELL TRADE process

    """
    global streaming_TRADE_first_index
    global streaming_STK_OPT_TRADE
    global stock_symbols_list
    global streaming_instrument_metadata

    #get stock index in streaming_STK_OPT_TRADE
    try:
        stock__index = stock_symbols_list.index(_stock)
    except:
        print('Stock not in the main list... exit with error (return = 0)')
        return 0 # error flag

    # clear instrument to reset
    _reqId = TRADE_slot_nb + streaming_TRADE_first_index

    # set streaming_STK_OPT_TRADE current TRADE rows values to zero
    streaming_STK_OPT_TRADE[_reqId,0] = 0
    streaming_STK_OPT_TRADE[_reqId,1] = 0
    streaming_STK_OPT_TRADE[_reqId,2] = 0
    streaming_STK_OPT_TRADE[_reqId,3] = 0
    streaming_STK_OPT_TRADE[_reqId,4] = 0
    streaming_STK_OPT_TRADE[_reqId,5] = 0

    app.cancelMktData(_reqId)
    time.sleep(2.0)

    streaming_instrument_metadata[_reqId] = None

    return 1


def fcn_isNYSE_open(now = None):
    global tz

    if not now:
        now = datetime.datetime.now(tz)
    # openTime = datetime.time(hour = 9, minute = 32, second = 0)
    openTime = datetime.time(hour = 9, minute = 0, second = 0)
    closeTime = datetime.time(hour = 16, minute = 6, second = 0)
    # If before 0935 or after 1600
    if (now.time() < openTime) or (now.time() > closeTime):
        return False
    # If it's a weekend
    if now.date().weekday() > 4:
        return False

    return True




def option_expiration(date):
    day = 21 - (calendar.weekday(date.year, date.month, 1) + 2) % 7
    return datetime.datetime(date.year, date.month, day)




def fcn_convert_date_intoYahoo_fin_format(date_str_):

    curr_year = date_str_[:4]
    curr_month = date_str_[5:7]
    curr_day = date_str_[8:10]

    #reformat to yayoo_fin
    date_yahoo = curr_month+'/'+curr_day+'/'+curr_year
    return date_yahoo

def fcn_update_STK_OPT_vect():
    # used to update prices in streaming_STK_OPT_currentPrices_vect vector (need to be updates to compute proper option ratios)
    global streaming_STK_OPT_TRADE
    global streaming_STK_OPT_currentPrices_vect
    global option_symbols_list
    global streaming_STK_nb
    global streaming_OPT_nb

    # Update STK prices
    streaming_STK_OPT_currentPrices_vect[:streaming_STK_nb,0] = streaming_STK_OPT_TRADE[:streaming_STK_nb,0]
    for i in range(streaming_STK_nb, streaming_STK_nb+streaming_OPT_nb):
        # find underlying STK in ticker list and put this price at option line index in vector streaming_STK_OPT_currentPrices_vect
        stock_ = option_symbols_list[i-streaming_STK_nb][4:]
        stock__index = stock_symbols_list.index(stock_)
        streaming_STK_OPT_currentPrices_vect[i,0] = streaming_STK_OPT_TRADE[stock__index,0]
    print('[done] All STK prices updated in streaming_STK_OPT_currentPrices_vect.')

def fcn_options_ratio_screening(stocks_df, nbDays_history, option_minimum_delay_nb_days, s):
    """
    Single STK process to perform ratio computation


    Parameters
    ----------
    stocks_df : Pandas DataFrame
        df containning the list of stock that have options contracts
    nbDays_history : int
        Nb. days used to compute ATR.
    option_minimum_delay_nb_days : TYintPE
        The minimum number of days we want expiration od option contract.
    s : TYPE
        the line number in stocks_df we want to process (the actual stock informations we need to preceed).

    Returns
    -------
    None.

    """



    today = str(datetime.date.today()- datetime.timedelta(nbDays_history))

    date_start_yahoo = fcn_convert_date_intoYahoo_fin_format(today)

    stock_ = stocks_df['symbol'][s]


    try:

        #Convert the string to datetime format
        # price_historic_1day = get_data(stock_ , start_date = date_start_yahoo, interval='1d')

        # Compute ATR
        # ATR_val  = fcn_ATR_STK_single(stock_, nbDays_history)
        price_last = get_live_price(stock_)


        # --------- GET THE OPTION CONTRACT DATE
        date_optionExp = option_expiration(datetime.datetime.today()) # option date this month

        date_diff = date_optionExp - datetime.datetime.today() # number of days until option expiration (of this month...)

        # If the next std option expiration date is in less than the required minimum delay specified, use the next month option date
        if date_diff.days < option_minimum_delay_nb_days:
            # option date next month
            date_optionExp = option_expiration(datetime.datetime.today()+ datetime.timedelta(30))

        # --------- RETREIVE STRIKE PRICE (based on last days historic closing price) ---------------
        # retreive option price
        # dataOption = options.get_puts(stock_, '06/19/2020')
        date_optionExp_yahoo_format = fcn_convert_date_intoYahoo_fin_format(str(date_optionExp))
        dataOption = options.get_puts(stock_, date_optionExp_yahoo_format)


        # find strike price just BELOW the last historic price
        for i in range(0,len(dataOption)):

            option_strike = dataOption['Strike'][i]
            if option_strike > price_last:
                #use previous strike price because we want to have a PUT just ABOVE the strike price
                # option_strike = dataOption['Strike'][i-1]
                option_strike_above = dataOption['Strike'][i]



                # compute the option ratio (using interpolation on current price = strike price)
                # last_PUT_price = dataOption.iloc[i-1,3]

                last_PUT_price_above = dataOption.iloc[i,3]

                # xp = [option_strike, option_strike_above]
                # fp = [last_PUT_price, last_PUT_price_above]

                # #interpolate option price to match strike price
                # last_PUT_price_interp = np.interp(price_last, xp, fp)
                # OPT_ratio = (ATR_val / last_PUT_price_interp ) * 100.0


                # compute the option ratio (using interpolation on current price = strike price)
                opt_intrinsic_val = option_strike_above - price_last
                opt_extrinsic_val = last_PUT_price_above - opt_intrinsic_val
                # OPT_ratio = (ATR_val / opt_extrinsic_val ) * 100.0

                # update df only if all data is present
                if dataOption['Bid'][i] != 0.0 and dataOption['Ask'][i] != 0.0 and last_PUT_price_above != 0.0 and ATR_val != 0.0:
                    # stocks_df.at[s,'option_ratio'] = OPT_ratio
                    stocks_df.at[s,'option_ratio'] = 0
                    IV_val = float(dataOption.iloc[i,10][:-1])
                    stocks_df.at[s,'IV'] = IV_val
                    stocks_df.at[s,'last_STK_price'] = price_last
                    stocks_df.at[s,'option_bid'] = dataOption['Bid'][i]
                    stocks_df.at[s,'option_ask'] = dataOption['Ask'][i]


                    # stocks_df.at[s,'option_spread'] = dataOption['Ask'][i-1] - dataOption['Bid'][i-1]
                    stocks_df.at[s,'option_spread'] = dataOption['Ask'][i] - dataOption['Bid'][i]
                    stocks_df.at[s,'option_strike'] = option_strike_above
                    stocks_df.at[s,'last_OPT_price'] = last_PUT_price_above
                    # stocks_df.at[s,'ATR'] = ATR_val
                    # stocks_df.at[s,'last_OPT_price_interp'] = last_PUT_price_interp


                    stocks_df.at[s,'option_expDate'] = str(dataOption.iloc[i,0])
                    print('STK '+stock_+' --- option ratio = '+ str(OPT_ratio) + '   IV: '+str(IV_val)+'%')



                    break

    except:
        print('Missing infos...   Skipping STK: '+stock_)


    if stocks_df['option_expDate'][s] == 'OPT_date':
        stocks_df.at[s,'option_ratio'] = 0.00

    return



# Below is the program execution

if __name__ == '__main__':

    print("before start")

    # Specifies that we are on local host with port 7497 (paper trading port number)
    app = TestApp("127.0.0.1", 7497, 0)  # TWS Station
    # app = TestApp("127.0.0.1", 4001, 0)    # IB Gateway

    time.sleep(5)

    # A printout to show the program began
    print("The program has begun")

    #assigning the return from our clock method to a variable
    requested_time = app.server_clock()

    #printing the return from the server
    print("")
    print("This is the current time from the server " )
    print(requested_time)

    #disconnect the app when we are done with this one execution
    # app.disconnect()

# Below is the input area


# Below is the logic processing area


# Calls the order execution function at the end of the program

app.account_update()    # Call this whenever you need to start accounting data
app.position_update()   # Call for current position
# app.price_update()

time.sleep(3)   # Wait three seconds to gather initial information


print(positionsDict)
"""
exemple
{'USD': {'positions': 3000.0, 'avgCost': 1.32413795}, 'COST': {'positions': 30.0, 'avgCost': 357.72}}
"""

for key, value in positionsDict.items():
    parsedSymbol = key
    parsedQuantity = value['positions']
    parsedCost = value['avgCost']
    print(str(parsedSymbol) + " " + str(parsedQuantity) + " " + str(parsedCost))


# #Total funds in USD:
# AccountFunds_USD = positionsDict['USD']['positions']
# print(AccountFunds_USD)

"""

AccountSummaryEnd. Req Id:  9004
{'BRK B': {'positions': 100.0, 'avgCost': 259.69505}, 'USD': {'positions': 3000.0, 'avgCost': 1.32413795}, 'COST': {'positions': 30.0, 'avgCost': 357.75333335}}
BRK B 100.0 259.69505
USD 3000.0 1.32413795



"""


# simulate a TRADE option on symbol 'TSLA'

#TRADE_infos = fcn_DataStreaming_start_OPT_TRADE('TSLA', 8, 0)
#
#fcn_DataStreaming_stop_OPT_TRADE('TSLA', 1)

##contractOption = contract_Option('AAPL')
#contractOption = contract_Option_PUT('AAPL')
#
#app.reqContractDetails(20, contractOption)

# to cancell a market data line
#appgr.cancelMktData(100)
# for i in range(0,50):
#     app.cancelMktData(i)
#     time.sleep(.1)
#app.cancelMktData(100)
#app.cancelMktData(100)
#app.cancelMktData(100)
#app.cancelMktData(100)
#app.cancelMktData(100)
#app.cancelMktData(100)
#app.cancelMktData(100)
#app.cancelMktData(100)

"""
orderExecution(symbolEntered, _qty)

orderExecution_PEG_MID('BRK B', 100, 0.1, 261.00)

"""

# ######################################################################################################################### !!!
# ############################################## MAIN TRADING SECTION #####################################################
# #########################################################################################################################

today_date = str(datetime.date.today())
logger.info(today_date+' ############################################## /// STARTING PROCESS /// ##############################################')

#  ------- Input variables -------
Delay_PROCESS_DATA_UPDATE = 10  # desired main time sleep (sec.) in send data refresh... put simply: the refresh rate of sended data to processing node...
# Delay_PROCESS_MESSAGE_UPDATE = 60  # desired main time sleep (sec.) in incoming message pooling... put simply: the refresh rate of sended data to processing node...
CTN_Display_refresh_nb = 90 # desired refresh period displaying informations on screen based in nb. Delay_PROCESS_DATA_UPDATE. ex: CTN_Display_refresh_nb = 6 ---> run this process each minutes because main loop is running each 10 seconds (6 * 10 = 60 sec = 1 minute).
TRADE_nbMAX_slots = 10  # the number of trade in same time
opt_expDelay_nDays = 12 # desired OPTION expiration delay in days


# STK_bloc_load_nb = 50  # the number of ticker we load at each time we update prices...
# DataMatrix_NbCols = 2500 # number of data points we keep in analysis


# # ------------------------------------ Connect to DynamoDB ------------------------------------
# with open('/home/manu/Documents/2009___GENESIS/IB/Code/KalmanSettup.txt') as f:
#     lines = f.readlines()

# dynamodb = boto3.resource(service_name = 'dynamodb',region_name = 'ca-central-1',
#               aws_access_key_id = lines[0][3:-1],
#               aws_secret_access_key = lines[1][:-3])

# table = dynamodb.Table('tg_data_tb')
# ------------------------------------ -------------------- ------------------------------------

# s3 = boto3.resource(
#     's3',
#     region_name='ca-central-1',
#     aws_access_key_id = lines[0][3:-1],
#     aws_secret_access_key = lines[1][:-3])



# bucket_name = 'mnu-tg-bucket'


today_date = str(datetime.date.today())
# screening_CTN = -180  #if negative: delay imposed before we start the stock srenning processs, -180 means .5 hour (on a 360 updates per hour basis)


CTN_retry_streaming = 0
flag_chec_dataArrayIsFull = 0 # to check if IB data streaming process has done all required STK and OPT as planned!!!
#Telegram infos to be able to send stuff...
# Email user infos loading for futur message send and retreival...
# with open('/home/manu/Documents/2009___GENESIS/IB/TG/TG_240508_base/LogInfos.txt', 'r') as fp:
#     LogInfos_data = fp.read().split("\n")
#     data_tk = LogInfos_data[6]
#     TOKEN = LogInfos_data[12]
#     CHAT_ID = int(LogInfos_data[13] )
# print(data_tk)
# print(TOKEN)
# print(CHAT_ID)


CHAT_ID_debugg = 7332899055 #Myself
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
data_tk = os.getenv("data_tk")
CHAT_ID = int(os.getenv("CHAT_ID"))





# db_size = os.path.getsize('/home/manu/Documents/2009___GENESIS/IB/Code/data/GENESIS_DataHistory.db')
flag_vol_consecutive_same = True # use to track if in two consecutives proce update, the STK volume section has change or not... to detect id market are closed in holllidays...

# Load stock list and option list to track in IB streaming process

# with open('/home/manu/Documents/2009___GENESIS/IB/Code/GEN_STK_HistoryList_500_210223.txt') as fp:
# with open('/home/manu/Documents/2009___GENESIS/IB/Code/GEN_STK_HistoryList_500_201229.txt') as fp:
# with open('/home/manu/Documents/2009___GENESIS/IB/TG/TG_240508_base/GEN_IB_STK_list.txt') as fp:
with open('/Users/manu/Documents/code/TG_project/TG_base/GEN_IB_STK_list.txt') as fp:
    stock_symbols_list = fp.read().split("\n")
    del stock_symbols_list[-1] #remove blank space at the end....




STK_bloc_id = 0

# stock_symbols_list  = stock_symbols_FULL_list[(STK_bloc_id)*STK_bloc_load_nb:(STK_bloc_id+1)*STK_bloc_load_nb]  # init with just first 50 STK from 500 full list


# stock_symbols_list_strConv = ','.join(stock_symbols_list)
# stock_symbols_FULL_list_strConv = ','.join(stock_symbols_FULL_list)

# Create a log file for real time messaging tracking
# ToDo....



with open('/Users/manu/Documents/code/TG_project/TG_base/GEN_IB_OPT_list.txt') as fp:
    option_symbols_list = fp.read().split("\n")
    del option_symbols_list[-1] #remove blank space at the end....

streaming_STK_nb = len(stock_symbols_list) # number of stock we follow using market data
streaming_OPT_nb = len(option_symbols_list) # number of option we follow using market data (using same sequence as stock listing... si if follow = 10, il will follow option prices of the first 10 stocks as shown in the ticker listing...)
streaming_TRADE_first_index =  streaming_STK_nb + streaming_OPT_nb

ATR_STK_vect = np.zeros((streaming_STK_nb+streaming_OPT_nb,1))

#stocks_df = pd.read_pickle('GENESIS_createTickerList_201109_Final_df.pkl')
#stock_symbols_list = list(stocks_df['symbol'])
#stock_symbols_list.insert(0, 'TICKER')  # add a TICKER header at first position
streaming_STK_OPT_TRADE = np.zeros((streaming_STK_nb+streaming_OPT_nb+TRADE_nbMAX_slots,6))
streaming_instrument_metadata = [None] * (streaming_STK_nb + streaming_OPT_nb + TRADE_nbMAX_slots)
# STK_vol_matrix = np.zeros((streaming_STK_nb,2))
# STK_vol_matrix_update = np.zeros((streaming_STK_nb,2))
daily_full_data_slices = []
daily_full_data_slices_unixtime = []
daily_full_data_slices_str_time = []

# bid_price_list = []
# ask_price_list = []
# bid_size_list = []
# ask_size_list = []

option_contractDate_list = option_symbols_list.copy() #use to store the option contract date expiration
streaming_STK_OPT_currentPrices_vect = np.zeros((streaming_STK_nb+streaming_OPT_nb,1)) # all STK prices vector (options contract prices, if streameing... being the underlying STK proce...)

TimeStamp_Start_PROCESS_DATA_UPDATE = time.time()   #Time to track the main price update process
TimeStamp_Start_PROCESS_MESSAGE_UPDATE = time.time()   #Time to track the message poolinge update process
TimeStamp_start_PROCESS_FUNDS_UPDATE = time.time() #Timer to track
TimeStamp_start_PROCESS_BUY_SELL_CHECK  = time.time() #Timer to track

updatePerDay = (60 / Delay_PROCESS_DATA_UPDATE ) * 60 * 6.5

tz = pytz.timezone('US/Eastern')
flag_doProcess = 0
CTN_Display = 0

state_isNYSE_open = fcn_isNYSE_open()
state_isNYSE_open_prev = state_isNYSE_open
timeStamp_unix = int(datetime.datetime.now(tz).timestamp())
date_now = datetime.datetime.now(tz)


# ---------------------- Real-Time stock volatility ---> Trigger message section --------------------
# Set thresholds for testing
triggerVolatilityMessage_window = 5  # Time window used to check volatility in all STK
triggerVolatilityMessage_window_tr = triggerVolatilityMessage_window * 60 / Delay_PROCESS_DATA_UPDATE  # in number of price refresh itertions at current update frequency
triggerVolatilityMessage_window_CTN = 0
# triggerVolatilityMessage_volTreshold = 0.0035  # 0.35% of volatility variation
# triggerVolatilityMessage_volTreshold = 0.0025  # 0.25% of volatility variation
triggerVolatilityMessage_volTreshold = 0.004  # 0.40% of volatility variation

# Variable in Update historic prices at each time "flag_doProcess == 1"
price_filter_nb_points = 6.0 #we will filetr prices to avoid get a spike min or max that d not last... so here we average price fluctuation , ex, if "price_filter_nb_points = 6" ---> using a 1/6 weight of latest value and 5/6 previous value
CORR_tr = 0.75

# Historical prices storage: numpy array with 3 columns (latest price, min price, max price)
# historical_prices = np.zeros((streaming_STK_nb, 4))
historical_prices = np.zeros((streaming_STK_nb, 5)) # ABS price change volatility added  as the last column

# Send a start up message to Telegram to confirm proper start up
mess_txt = 'RealTimeData /// started! :P'
url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID_debugg}&text={mess_txt}"
r = requests.get(url)


# At start-up : check if Sqlite db exist (if not, create one with a dummy first row of data)
PathCurrent = os.getcwd()
# change current working path to 'data' folder
os.chdir('/Users/manu/Documents/code/TG_project/TG_base/data')

# if os.path.isfile('GENESIS_DataHistory.db'):
#     print('\n[ok] GENESIS_DataHistory.db exist in data folder.\n')
# else:
#     print('\n[create] GENESIS_DataHistory.db in data folder...\n')
#     fcn_HistoricData_create_Sqlite_db()

# GEN_OptionInfos_df_exist = False
# # load specific option infprmations to continue price update
# if os.path.isfile('GEN_OptionInfos_df.pkl'):
#     print('\n[ok] GEN_OptionInfos_df exist in data folder.\n')
#     #Load file infos in df
#     CurrentOptionInfos_df = pd.read_pickle('GEN_OptionInfos_df.pkl')
#     GEN_OptionInfos_df_exist = True
# else:
#     print('\n[warning] GEN_OptionInfos_df DO NOT exist in data folder.\n')
#     fcn_HistoricData_create_Sqlite_db()

# CurrentOptionInfos_df = pd.read_pickle('GEN_OptionInfos_df.pkl'
# #/home/manu/Documents/2009___GENESIS/IB/Code/data/GEN_OptionInfos_df.pkl

# #################################
#     ATR : UPDATE ALL STOCKS VALUES
# #################################
# fcn_ATR_update_All(ATR_nbDays_w)

# At start-up activate Market Data Streaming process
# #################################
#     STREAMING : ALL STOCKS
# #################################
fcn_DataStreaming_start_STK_All()

# Seed 0DTE option streaming (ATM/OTM Calls & Puts) for the chosen underlying
UNDERLYING_0DTE_SYMBOL = "SPY"  # switch to "SPX" or another symbol if desired
UNDERLYING_0DTE_STRIKE_STEP = 1.0  # adjust for instruments with different strike increments
UNDERLYING_0DTE_OTM_OFFSET = 5.0   # proxy distance from ATM (~0.25 delta)
UNDERLYING_0DTE_TRADING_CLASS = None  # e.g. "SPXW" for SPX weeklys

try:
    option_metadata_0dte = add_0dte_option_contracts(
        streaming_table=streaming_STK_OPT_TRADE,
        stock_symbols=stock_symbols_list,
        option_contract_dates=option_contractDate_list,
        underlying=UNDERLYING_0DTE_SYMBOL,
        strike_step=UNDERLYING_0DTE_STRIKE_STEP,
        otm_offset=UNDERLYING_0DTE_OTM_OFFSET,
        trading_class=UNDERLYING_0DTE_TRADING_CLASS,
    )
    logger.info("{} 0DTE contracts attached: {}", UNDERLYING_0DTE_SYMBOL, option_metadata_0dte)
    fcn_DataStreaming_start_0DTE_options(option_metadata_0dte, generic_tick_list="106")
except Exception as exc:
    logger.error("Failed to seed {} 0DTE contracts: {}", UNDERLYING_0DTE_SYMBOL, exc)


# !!! Check all available stock infos and fill data_2save with the first iteration data
time.sleep(3)


#######################################################################################
#           Filter stock that are correctly streamed and keep this list in a separate variable
#######################################################################################
# if state_isNYSE_open == True:

#check all sSTK with non zero values ---. ther are updating!!! We want to save them at the end of the day
data_2save_STK_list = []
data_2save_STK_list_idx = []
data_2save_OPT_list = []
data_2save_OPT_list_idx = []

for i in range(0,len(stock_symbols_list)):

    if streaming_STK_OPT_TRADE[i,0] != 0 and streaming_STK_OPT_TRADE[i,1] != 0 and streaming_STK_OPT_TRADE[i,3] != 0 :

        # Add ticker to header list
        data_2save_STK_list.append(stock_symbols_list[i])
        # Add ticker index to know the proper row to query in the futur
        data_2save_STK_list_idx.append(i)

for j in range(streaming_STK_nb, streaming_STK_nb + streaming_OPT_nb + TRADE_nbMAX_slots):
    meta = streaming_instrument_metadata[j]
    if meta is None:
        continue
    if meta.get("type", "").startswith("OPT"):
        if (streaming_STK_OPT_TRADE[j,0] != 0 and streaming_STK_OPT_TRADE[j,1] != 0):
            strike = meta.get("strike")
            try:
                strike_str = f"{float(strike):.2f}" if strike is not None else "NA"
            except (TypeError, ValueError):
                strike_str = str(strike)
            opt_label = f"OPT_{meta.get("right", "U")}_{meta.get("symbol", "UNK")}_{strike_str}"
            data_2save_OPT_list.append(opt_label)
            data_2save_OPT_list_idx.append(j)
        data_2save_STK_list.append(stock_symbols_list[i])
        # Add ticker index to know the proper row to query in the futur
        data_2save_STK_list_idx.append(i)

logger.info(' ')
logger.info('data_2save_STK_list')
logger.info(data_2save_STK_list)
logger.info(' ')
logger.info('data_2save_STK_list_idx')
logger.info(data_2save_STK_list_idx)
logger.info('data_2save_OPT_list')
logger.info(data_2save_OPT_list)
logger.info('data_2save_OPT_list_idx')
logger.info(data_2save_OPT_list_idx)




while(1):  #MAIN LOOP


    # refresh timeStamp time at each iteration # Ex: 1605198449
    timeStamp_unix = int(datetime.datetime.now(tz).timestamp())

    timeStamp_unix_str = str(timeStamp_unix)

    # if time.time() - TimeStamp_Start_PROCESS_DATA_UPDATE >= Delay_PROCESS_DATA_UPDATE:
    #     TimeStamp_Start_PROCESS_DATA_UPDATE = time.time()

    ### Forcing the main price update to run at each 10 seconds using unix time for better process sync outside this script
    if timeStamp_unix_str[-1] == '0':

        state_isNYSE_open_prev = state_isNYSE_open
        state_isNYSE_open = fcn_isNYSE_open()

        if state_isNYSE_open == True:
            flag_doProcess = 1
        else:
            flag_doProcess = 0
            # print(datetime.datetime.now(tz))

    #!!! ***********************************************************************
    #                       PROCESS 1 : DATA UPDATE
    # ***********************************************************************
    if flag_doProcess == 1:
        flag_doProcess = 0 # reset

        CTN_Display += 1 # use to display informations at a regular interval set by 'CTN_Display_refresh_nb'

        # refresh date time at each iteration
        date_now = datetime.datetime.now(tz)  # Ex: datetime.datetime(2020, 11, 12, 11, 26, 19, 647227, tzinfo=<DstTzInfo 'US/Eastern' EST-1 day, 19:00:00 STD>)



        triggerVolatilityMessage_window_CTN += 1 # update counter for real time trigger process
        # Do real time price min $ max update

        # Accumulate full data at each main iteration (daily_full_data_slices and daily_full_data_slices_unixTime)
        daily_full_data_slices_unixtime.append(timeStamp_unix)


        daily_full_data_slices_str_time.append(f'{datetime.datetime.now()}'[:-7])


        # Data col srtructure : [ latestPrice, Bid, Ask, Vol, Bid_size, Ask_size]
        curr_dataSlice = streaming_STK_OPT_TRADE[data_2save_STK_list_idx,:]

        # Reset accumulation of bid and ask size values
        streaming_STK_OPT_TRADE[:,[4,5]] = 0.0

        print(data_2save_STK_list)
        print(curr_dataSlice)
        daily_full_data_slices.append(curr_dataSlice)

        # Save in a file talest data (overwrite file at each iterations)
        # Create the DataFrame from numpy array
        curr_dataSlice_df = pd.DataFrame(np.array(curr_dataSlice), columns=[ 'latestPrice', 'Bid', 'Ask', 'Vol', 'Bid_size', 'Ask_size'], index=data_2save_STK_list)

        #Add stockName in df
        curr_dataSlice_df['stk_name'] = data_2save_STK_list
        curr_dataSlice_df['ts_unix'] = timeStamp_unix
        #Save file
        print(' ')
        print('###   saving latest data info TG_RT_latestData.parquet file   ###')
        print(' ')

        curr_dataSlice_df.to_parquet('/Users/manu/Documents/code/TG_project/TG_base/data/TG_RT_latestData.parquet', index=False)

        if CTN_Display == 60:
            logger.info(curr_dataSlice_df)


        # Delay to make sure we avoid re-propcessing this price update many times (process triggered at each 10 seconds and pooling at each 0.25 sec...)
        time.sleep(2)



    # ***********************************************************************
    #           Saving Real Time Data (price and volume) to DynamoDB & DISPLAY INFOS ...
    # ***********************************************************************
    if CTN_Display >= CTN_Display_refresh_nb:
        CTN_Display = 0 # reset...



        # Debugg --------------------------------------------------------------------------------------------------------------------------------
        # Stack and save
        try:
            print(today_date+' ------ SAFE SAVING --------->>> /// SAVING FULL DATA in single file .npz zipped archive in a cumulative sequence...')
            # Stacking with the new dimension at the end (Z-axis last)
            daily_full_data_slices_stacked_3d = np.stack(daily_full_data_slices, axis=2)


            print(' ')

            np.savez('TG_dataHistory_1d_10secRefresh_fullData__TEST_'+today_date+'.npz',
                     daily_full_data_3d = daily_full_data_slices_stacked_3d,
                     unixTime = np.array(daily_full_data_slices_unixtime),
                     strTime = np.array(daily_full_data_slices_str_time),
                     tickerNames = np.array(data_2save_STK_list) )

            logger.info('saving test .npz... done!')

        # # Send a start up message to Telegram to confirm proper start up
        # mess_txt = 'RealTimeData /// Closing !'
        # url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID_debugg}&text={mess_txt}"
        # r = requests.get(url)

        except Exception as e:
            logger.error(f"Test in day - SAVING FULL DATA in single file .npz error: {e}")
            print('[ERROR] In testing... Not able to SAVE FULL DATA in single file .npz zipped archive at CLOSING of exchange... check why... You IDOT!!!')



    # ***********************************************************************
    #           UPDATE PERFORMED ONCE per day at OPENING of exchange
    # ***********************************************************************
    if state_isNYSE_open == True and state_isNYSE_open_prev == False :
        state_isNYSE_open_prev = state_isNYSE_open
        state_isNYSE_open = fcn_isNYSE_open()
        print('UPDATE PERFORMED ONCE per day at OPENING of exchange')
        print(str(timeStamp_unix),' - ### START OF DAY flag_vol_consecutive_same: ', flag_vol_consecutive_same)



        if flag_vol_consecutive_same == True: # SKT prices are not changing...
            mess_txt = 'GENESIS /// SKT prices are not changing... check if TODAY == Hollyday ? ... you IDOT!!!'
            #fcn_Telegram_send(mess_txt, data_tk)  # message simples


        # Init variables
        # STK_vol_matrix = np.zeros((streaming_STK_nb,2)) # EACH MORNING, set matrix to zero to start day exchange with a zero starting volume



        if date_now.date().weekday() == 0:
            print('UPDATE PERFORMED ONLY ON MONDAY at OPENING of exchange')
            mess_txt = 'GENESIS /// MONDAY Option contracts update to current strike price...'
            #fcn_Telegram_send(mess_txt, data_tk)  # message simples

            # Refresh Option contract with a strike price at current stocks value
            # fcn_DataStreaming_start_OPT_SetStrike_All()

        flag_chec_dataArrayIsFull = 0 # init at each morning...

        # ---------------------------------------------- Init data for later save ----------------------------------------------

        #check all sSTK with non zero values ---. ther are updating!!! We want to save them at the end of the day
        data_2save_STK_list = []
        data_2save_STK_list_idx = []

        for i in range(0,len(stock_symbols_list)):

            if streaming_STK_OPT_TRADE[i,0] != 0 and streaming_STK_OPT_TRADE[i,1] != 0 and streaming_STK_OPT_TRADE[i,3] != 0 : #Save all ticker that are availables
            # if streaming_STK_OPT_TRADE[i,0] != 0 and streaming_STK_OPT_TRADE[i,1] != 0 and streaming_STK_OPT_TRADE[i,3] != 0 : #Save only specific tickers (to save on DynamoDB access...)

                # Add ticker to header list
                data_2save_STK_list.append(stock_symbols_list[i])
                # Add ticker index to know the proper row to query in the futur
                data_2save_STK_list_idx.append(i)

        print(' ')
        print(' ---------------------------------------------- Morning sequence----------------------------------------------')
        print('data_2save_STK_list')
        print(data_2save_STK_list)
        print(' ')
        print('data_2save_STK_list_idx')
        print(data_2save_STK_list_idx)





    # ***********************************************************************
    #           UPDATE PERFORMED ONCE per day at CLOSING of exchange
    # ***********************************************************************

    if state_isNYSE_open == False and state_isNYSE_open_prev == True :
        state_isNYSE_open_prev = state_isNYSE_open
        state_isNYSE_open = fcn_isNYSE_open()

        print('UPDATE PERFORMED ONCE per day at CLOSING of exchange')
        print(str(timeStamp_unix),' - ### END of day : flag_vol_consecutive_same: ', flag_vol_consecutive_same)

        mess_txt = 'GENESIS /// CLOSING of exchange'
        # fcn_Telegram_send(mess_txt, data_tk)  # message simples



        # Stack and save
        try:
            print(today_date+' --------------->>> /// SAVING FULL DATA in single file .npz zipped archive at CLOSING of exchange /// ....')
            # Stacking with the new dimension at the end (Z-axis last)
            daily_full_data_slices_stacked_3d = np.stack(daily_full_data_slices, axis=2)
            print(' ')
            print('daily_full_data_slices_stacked_3d')
            print(daily_full_data_slices_stacked_3d)

            print(np.array('daily_full_data_slices_unixtime'))
            print(np.array(daily_full_data_slices_unixtime))
            print(np.array('daily_full_data_slices_str_time'))
            print(np.array(daily_full_data_slices_str_time))
            print(np.array('data_2save_STK_list') )
            print(np.array(data_2save_STK_list) )
            print(' ')

            # saving

            np.savez('TG_dataHistory_1d_10secRefresh_fullData__'+today_date+'.npz',
                     daily_full_data_3d = daily_full_data_slices_stacked_3d,
                     unixTime = np.array(daily_full_data_slices_unixtime),
                     strTime = np.array(daily_full_data_slices_str_time),
                     tickerNames = np.array(data_2save_STK_list) )

            print('saving... done!')

        # # Send a start up message to Telegram to confirm proper start up
        # mess_txt = 'RealTimeData /// Closing !'
        # url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID_debugg}&text={mess_txt}"
        # r = requests.get(url)

        except Exception as e:
            logger.error(f"End of day - SAVING FULL DATA in single file .npz error: {e}")
            print('[ERROR] Not able to SAVE FULL DATA in single file .npz zipped archive at CLOSING of exchange... check why... You IDOT!!!')

        # except:
        #     print('[ERROR] Not able to SAVE FULL DATA in single file .npz zipped archive at CLOSING of exchange... check why... You IDOT!!!')

        time.sleep(2.0)


        # try:
        #     # ---------------- Save ibkr price data to S3 ---------------- -------------> to implement by saving the  .npz generated in the previous lines
        #     # Create a bytes buffer
        #     pkl_buffer = io.BytesIO()

        #     # Save DataFrame to the buffer
        #     df_data_2save_price.to_pickle(pkl_buffer)

        #     # Move cursor to the beginning of the buffer
        #     pkl_buffer.seek(0)

        #     # Get the bucket
        #     bucket = s3.Bucket(bucket_name)

        #     file_name = 'TG_dataHistory_1d_05secRefresh_price__'+today_date+'.pkl'
        #     s3_path = f"tg_export/ibkr_data/{file_name}"

        #     # Upload to S3
        #     bucket.upload_fileobj(
        #     pkl_buffer,
        #     s3_path
        #     )
        # except:
        #     print('[ERROR] Not able to save ibkr price data to S3 ')

        # try:
        #     # ---------------- Save ibkr volume data to S3 ----------------
        #     # Create a bytes buffer
        #     pkl_buffer = io.BytesIO()

        #     # Save DataFrame to the buffer
        #     df_data_2save_vol.to_pickle(pkl_buffer)

        #     # Move cursor to the beginning of the buffer
        #     pkl_buffer.seek(0)

        #     # Get the bucket
        #     bucket = s3.Bucket(bucket_name)

        #     file_name = 'TG_dataHistory_1d_05secRefresh_vol__'+today_date+'.pkl'
        #     s3_path = f"tg_export/ibkr_data/{file_name}"

        #     # Upload to S3
        #     bucket.upload_fileobj(
        #     pkl_buffer,
        #     s3_path
        #     )
        # except:
        #     print('[ERROR] Not able to save ibkr price data to S3 ')


        # Disconnect from the IB server
        app.disconnect()

        print("Exiting main program...")
        sys.exit(0)





    #Main loop sleep delay
    time.sleep(0.25)


"""
 #################### Price & Volume UPDATE ####################
2024-09-03 12:39:13.386360




['SPY', 'SPXL', 'SPXS', 'VIXY', 'SVXY', 'TQQQ', 'SOXS', 'SOXL', 'UVIX', 'BITX', 'QQQ', 'IWM', 'HYG', 'LQD']
data = [[6.46030e+02 6.46020e+02 6.46030e+02 2.07312e+05 2.60000e+01 5.60000e+01],
 [1.93680e+02 1.93610e+02 1.93620e+02 9.87200e+03 1.00000e+00 3.00000e+00],
 [4.20000e+00 4.19000e+00 4.20000e+00 1.92051e+05 7.21600e+03 2.79800e+03],
 [3.48100e+01 3.48000e+01 3.48200e+01 5.57100e+03 9.00000e+00 7.00000e+00],
 [4.90700e+01 4.90700e+01 4.90800e+01 4.19700e+03 1.00000e+00 1.80000e+01],
 [9.00500e+01 9.00400e+01 9.00500e+01 2.69765e+05 1.45000e+02 2.01000e+02],
 [7.41000e+00 7.40000e+00 7.41000e+00 5.84320e+05 1.87800e+03 3.00800e+03],
 [2.51000e+01 2.51000e+01 2.51100e+01 4.18365e+05 7.80000e+01 8.70000e+01],
 [1.17900e+01 1.17800e+01 1.17900e+01 8.86390e+04 7.90000e+01 5.40000e+01],
 [5.18300e+01 5.18200e+01 5.18300e+01 2.28120e+04 1.00000e+00 2.20000e+01],
 [5.72090e+02 5.72090e+02 5.72100e+02 1.85197e+05 3.00000e+00 3.30000e+01],
 [2.34610e+02 2.34610e+02 2.34620e+02 1.10753e+05 3.40000e+01 3.60000e+01],
 [8.06700e+01 8.06600e+01 8.06700e+01 8.89600e+04 1.06200e+03 1.38400e+03],
 [1.10080e+02 1.10070e+02 1.10080e+02 1.47353e+05 2.32000e+02 1.05700e+03]]

data = [[6.46030e+02, 6.46020e+02, 6.46030e+02, 2.07312e+05, 2.60000e+01, 5.60000e+01],
        [1.93680e+02, 1.93610e+02, 1.93620e+02, 9.87200e+03, 1.00000e+00, 3.00000e+00],
        [4.20000e+00, 4.19000e+00, 4.20000e+00, 1.92051e+05, 7.21600e+03, 2.79800e+03],
        [3.48100e+01, 3.48000e+01, 3.48200e+01, 5.57100e+03, 9.00000e+00, 7.00000e+00],
        [4.90700e+01, 4.90700e+01, 4.90800e+01, 4.19700e+03, 1.00000e+00, 1.80000e+01],
        [9.00500e+01, 9.00400e+01, 9.00500e+01, 2.69765e+05, 1.45000e+02, 2.01000e+02],
        [7.41000e+00, 7.40000e+00, 7.41000e+00, 5.84320e+05, 1.87800e+03, 3.00800e+03],
        [2.51000e+01, 2.51000e+01, 2.51100e+01, 4.18365e+05, 7.80000e+01, 8.70000e+01],
        [1.17900e+01, 1.17800e+01, 1.17900e+01, 8.86390e+04, 7.90000e+01, 5.40000e+01],
        [5.18300e+01, 5.18200e+01, 5.18300e+01, 2.28120e+04, 1.00000e+00, 2.20000e+01],
        [5.72090e+02, 5.72090e+02, 5.72100e+02, 1.85197e+05, 3.00000e+00, 3.30000e+01],
        [2.34610e+02, 2.34610e+02, 2.34620e+02, 1.10753e+05, 3.40000e+01, 3.60000e+01],
        [8.06700e+01, 8.06600e+01, 8.06700e+01, 8.89600e+04, 1.06200e+03, 1.38400e+03],
        [1.10080e+02, 1.10070e+02, 1.10080e+02, 1.47353e+05, 2.32000e+02, 1.05700e+03]]

SINGLE array in a FILE
    import numpy as np
    my_3d_array = np.random.rand(10, 5, 3) # Example 3D array
    np.save('my_3d_array.npy', my_3d_array)
    loaded_array = np.load('my_3d_array.npy')



Multi object in a single file

#Saving
import numpy as np
my_3d_array = np.random.rand(10, 5, 3)
another_array = np.array([1, 2, 3])
np.savez('my_data.npz', array3d=my_3d_array, other_data=another_array)

#Loading
loaded_data = np.load('my_data.npz')
loaded_3d_array = loaded_data['array3d']
loaded_other_array = loaded_data['other_data']
loaded_data.close() # Important to close the file handle


---------------------- write parquet file -------------------------------------

import pandas as pd

# Create a sample DataFrame
data = {'col1': [1, 2, 3], 'col2': ['A', 'B', 'C']}
df = pd.DataFrame(data)

# Save the DataFrame to a Parquet file
# 'path/to/your/file.parquet' is the desired file path
df.to_parquet('my_dataframe.parquet', index=False)
# index=False prevents writing the DataFrame index to the Parquet file,
# which is often desired to avoid an unnecessary column.

import pandas as pd

# Load the Parquet file into a DataFrame
# 'path/to/your/file.parquet' is the path to the Parquet file
loaded_df = pd.read_parquet('my_dataframe.parquet')

# Display the loaded DataFrame
print(loaded_df)



"""
