import websocket, json, time
from binance.client import Client
from binance.enums import *
from binance.exceptions import BinanceAPIException
from binance.websockets import BinanceSocketManager
import pandas as pd
import numpy as np
from datetime import datetime
import csv
import logging,os
import random

api_key = 'Your API Key Here'
api_secret = 'Your API secret Here'

client = Client(api_key, api_secret)


def get_date(d):
    d= d /1000
    return  datetime.utcfromtimestamp(d)

back_tested = {'BUY_CLOSE_TIME' : [],
                'INIT_CASH' : [],
                'QUANTITY' : [],
                'PRICE_PAID' : [],
                'BUY_SPOT_PRICE' : [],
                'SELL_AMOUNT' : [],
                'SELL_SPOT_PRICE' : [],
                'SELL_CLOSE_TIME' : [],
                'PROFIT_LOSS' : [],
                'BUY_ORDER_ID':[],
                'SELL_ORDER_ID': []}


closes = []
sma5 = []
sma15 = []
in_position = False

###Variable

TRADE_SYMBOL = input()
INIT_CASH = float(client.get_asset_balance('USDT')['free'])
TAKE_PROFIT = 0.2
TAKE_LOSS = 0.3
BUY_COMMISSION = 0.1
SELL_COMMISSION = 0.1
LAST_SPOT_PRICE = 0
PROFIT_LOSS = 0
order_id = None
BUY_ORDER_ID = None
SELL_ORDER_ID = None
last_close = 0


order_csv_name = 'order_csv.csv'
order_csv = pd.DataFrame(columns = ['order_id','order_type'])
if os.path.isfile(order_csv_name):
    pass
else:
    order_csv.to_csv(order_csv_name,index=False)


logging.basicConfig(filename=TRADE_SYMBOL+'_GTC.log', format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)

logging.debug('INIT CASH is {0}'.format(INIT_CASH))

# back_tested['symbol'] = TRADE_SYMBOL
name = TRADE_SYMBOL + '_GTC.csv'
temp_df = pd.DataFrame(back_tested)
if os.path.isfile(name):
    pass
else:
    temp_df.to_csv(name,index=False)


logging.debug('Getting Historical Data')
##Feeding last 1 day data
hist = client.get_historical_klines(symbol=TRADE_SYMBOL,interval=Client.KLINE_INTERVAL_1MINUTE,start_str='1 Day')
for i in np.array(hist)[:,4]:
    closes.append(float(i))
    if len(closes)>15:
        sma5.append(sum(closes[-5:])/5)
        sma15.append(sum(closes[-15:])/15)


logging.debug('Got Historical Data')


def order(ORDER_TYPE,QUANTITY,PRICE_PAID):
        output = client.create_order(
        symbol=TRADE_SYMBOL,
        side=ORDER_TYPE,
        type=ORDER_TYPE_LIMIT,
        timeInForce=TIME_IN_FORCE_GTC,
        quantity=QUANTITY,
        price=str(PRICE_PAID))

        return output


#######################################################################################################################################

def process_message(msg):
    global closes, in_position, SELL_CLOSE_TIME, SELL_SPOT_PRICE, SELL_AMOUNT, QUANTITY, BUY_CLOSE_TIME, BUY_SPOT_PRICE, PRICE_PAID, INIT_CASH
    global PROFIT_LOSS, MINIMUM_SPOT_PRICE, EXPECTED_SELL_SPOT_PRICE, order_id, order_status, BUY_ORDER_ID,SELL_ORDER_ID,order_type,order_status
    global partial_output, last_close
    LAST_SPOT_PRICE = round(float(msg['k']['c']),4)


    if order_id is not None:
        try:
            local_order = client.get_order(symbol=TRADE_SYMBOL,orderId = order_id)
            order_status = local_order['status']
            order_type = local_order['side']
            executed_qty = float(local_order['executedQty'])

            logging.debug('Order Status is {0}'.format(order_status))
            logging.debug('Order Type is {0}'.format(order_type))

            if order_type=='BUY' and order_status=='FILLED':
                in_position = True
                logging.debug('Setting In Position {0}'.format(in_position))
            elif order_type=='BUY' and order_status=='NEW':
                if ( LAST_SPOT_PRICE>= BUY_SPOT_PRICE*(1.005) ) or ( LAST_SPOT_PRICE<= BUY_SPOT_PRICE*(.995) ):
                    logging.debug('Canceling the buy order as its not fullfilled ')
                    cancel_output = client.cancel_order(symbol=TRADE_SYMBOL,orderId = order_id)
                    logging.debug('Canceled the buy order  {0}'.format(cancel_output))
                    in_position = False
                    logging.debug('Setting In Position {0}'.format(in_position))
                else:
                    pass
            elif order_type=='BUY' and order_status=='PARTIALLY_FILLED':
                    in_position = True
                    QUANTITY = buy_output['executedQty']
                    logging.debug("Went into PARTIALLY_FILLED, cancelling the order")
                    partial_output = client.cancel_order(symbol=TRADE_SYMBOL,orderId = order_id)
                    logging.debug("Canceled partial order :: {0}".format(partial_output))

            elif order_type=='SELL' and order_status=='FILLED':
                logging.debug('Order Status is {0}'.format(order_status))
                logging.debug('Order Type is {0}'.format(order_type))
                logging.debug('executedQty is {0}'.format(executed_qty))
                #last_close = 1
                #logging.debug('last_close for sell is {0}'.format(last_close))
                if executed_qty == QUANTITY-1:
                    in_position = False
                    logging.debug('Setting In Position {0}'.format(in_position))
                    last_close = 1
                    logging.debug('last_close for sell is {0}'.format(last_close))
                    order_id=None
                else:
                    logging.debug("Canceling Selling order ")
                    in_position = True
                    QUANTITY = (QUANTITY -1) - executed_qty
                    partial_output = client.cancel_order(symbol=TRADE_SYMBOL,orderId = order_id)
                    logging.debug("Canceled Selling order :: {0}".format(partial_output))
            elif order_type=='SELL' and order_status=='PARTIALLY_FILLED':
                    logging.debug("Canceling Selling order in partial")
                    in_position = True
                    QUANTITY = (QUANTITY -1) - executed_qty
                    partial_output = client.cancel_order(symbol=TRADE_SYMBOL,orderId = order_id)
                    logging.debug("Canceled Selling order :: {0}".format(partial_output))
                    # time.sleep(60)
            elif order_type=='SELL' and order_status=='NEW':
                if ( LAST_SPOT_PRICE>= SELL_SPOT_PRICE*(1.005) ) or ( LAST_SPOT_PRICE<= SELL_SPOT_PRICE*(.995) ):
                    logging.debug('Canceling the buy order as its not fullfilled ')
                    cancel_output = client.cancel_order(symbol=TRADE_SYMBOL,orderId = order_id)
                    logging.debug('Canceled the SELL order  {0}'.format(cancel_output))
                    in_position = True
                    logging.debug('Setting In Position {0}'.format(in_position))
                else:
                    pass
            else:
                logging.debug('Order Status is {0} in else'.format(order_status))
                logging.debug('Current Position : {0}'.format(in_position))
                pass

        except Exception as e:
            logging.error('ERROR occured {0}'.format(e))

    else:
        pass




    info = client.get_symbol_info(TRADE_SYMBOL)
    min_qty = float(info['filters'][2]['minQty'])
    logging.debug('min_qty is {0} in else'.format(min_qty))
    min_notional = float(info['filters'][3]['minNotional'])
    logging.debug('min_notional is {0} in else'.format(min_notional))


    if INIT_CASH >=min_notional:

        if msg['k']['x'] == True:
            closes.append(float(msg['k']['c']))
            df = pd.DataFrame(data=closes,columns=['close'])
            # df.to_csv('last_data.csv')
            logging.debug('Last Close at {0}'.format(closes[-1]))
            # if len(closes)>15:
            sma5.append(sum(closes[-5:])/5)
            sma15.append(sum(closes[-15:])/15)
            if order_id==None:
                last_close = 0
                logging.debug('last_close for sell is {0}'.format(last_close))

        else:
            pass


        if order_id is None or order_status=='FILLED' or order_status=='CANCELED':
            logging.debug('OrderId Condition Met')
            if in_position:
                logging.debug('Checking Selling Condition')
                if LAST_SPOT_PRICE <= MINIMUM_SPOT_PRICE :
                    logging.debug('Selling')

                    sell_output = order(SIDE_SELL,QUANTITY-1,round(LAST_SPOT_PRICE*0.999,4))
                    order_id = sell_output['orderId']
                    logging.debug("Sell Order Id {0}".format(order_id))
                    SELL_ORDER_ID = order_id
                    order_status = sell_output['status']

                    SELL_AMOUNT = round(LAST_SPOT_PRICE * (QUANTITY-1),4)
                    SELL_CLOSE_TIME = get_date(float(msg['k']['t']))
                    SELL_SPOT_PRICE = LAST_SPOT_PRICE

                    back_tested['SELL_CLOSE_TIME'].append(SELL_CLOSE_TIME)
                    back_tested['SELL_SPOT_PRICE'].append(LAST_SPOT_PRICE)
                    back_tested['SELL_AMOUNT'].append(SELL_AMOUNT)
                    in_position = False

                    logging.debug('Sell Spot Price {0}'.format(SELL_SPOT_PRICE))
                    logging.debug('Sell At {0}'.format(SELL_AMOUNT))

                    PROFIT_LOSS = SELL_AMOUNT - PRICE_PAID
                    logging.debug("PROFIT_LOSS {0}".format(PROFIT_LOSS))

                    with open(name,'a') as f:
                        writer = csv.writer(f)
                        writer.writerow([BUY_CLOSE_TIME,INIT_CASH,QUANTITY,PRICE_PAID,BUY_SPOT_PRICE,SELL_AMOUNT,SELL_SPOT_PRICE,SELL_CLOSE_TIME,PROFIT_LOSS,BUY_ORDER_ID,SELL_ORDER_ID])

                    INIT_CASH = INIT_CASH + PROFIT_LOSS
                    logging.debug('INIT_CASH at : {0}'.format(INIT_CASH))

                    with open(order_csv_name,'a') as f:
                        writer = csv.writer(f)
                        writer.writerow([SELL_ORDER_ID,'sell'])
                    # logging.debug("Sleeping for 30 Sec")
                    # time.sleep(30)
                    # logging.debug("Waking after 30 Sec")

                elif  LAST_SPOT_PRICE >= EXPECTED_SELL_SPOT_PRICE:
                    MINIMUM_SPOT_PRICE = round(EXPECTED_SELL_SPOT_PRICE * 0.997,4)
                    EXPECTED_SELL_SPOT_PRICE = round(EXPECTED_SELL_SPOT_PRICE * 1.001,4)

                    logging.debug("MINIMUM_SPOT_PRICE {0}".format(MINIMUM_SPOT_PRICE))
                    logging.debug('New Expected Sell Amount{0}'.format(EXPECTED_SELL_SPOT_PRICE))

                else:
                    logging.debug("MINIMUM_SPOT_PRICE {0}".format(MINIMUM_SPOT_PRICE))
                    logging.debug('LAST_SPOT_PRICE {0}'.format(LAST_SPOT_PRICE))
                    logging.debug("Expected Spot Price {0}".format(EXPECTED_SELL_SPOT_PRICE))
        #             logging.debug()
                    pass

            else:
                logging.debug('Checking Buying Condition')
                if sma5[-1]>sma15[-1] and closes[-1]>closes[-2] and closes[-2]>closes[-3] and last_close==0:

                    logging.debug('INIT_CASH at : {0}'.format(INIT_CASH))
                    BUY_SPOT_PRICE = round(float(msg['k']['c']) * 1.001,4)
                    logging.debug('Buying at : {0}'.format(BUY_SPOT_PRICE))

                    QUANTITY = float(round((INIT_CASH / BUY_SPOT_PRICE)*0.95))
                    logging.debug("Buy Quantity :: {0}".format(QUANTITY))
                    if QUANTITY>=min_qty:

                        ### Buying Order ###
                        try:
                            buy_output = order(SIDE_BUY,QUANTITY,BUY_SPOT_PRICE)
                            order_id = buy_output['orderId']
                            logging.debug("Buy Order Id {0}".format(order_id))
                            logging.debug("buy_output :: {0}".format(buy_output))
                            BUY_ORDER_ID = order_id
                            order_status = buy_output['status']


                            logging.debug("Setting Variables like, price paid, etc")
                            logging.debug("BUY_SPOT_PRICE :: {0}".format(BUY_SPOT_PRICE))
                            logging.debug("QUANTITY :: {0}".format(QUANTITY))
                            logging.debug("BUY_COMMISSION :: {0}".format(BUY_COMMISSION))

                            PRICE_PAID = round((BUY_SPOT_PRICE*(1) * QUANTITY) * (1 + BUY_COMMISSION/100),4)
                            logging.debug("PRICE_PAID :: {0}".format(PRICE_PAID))

                            BUY_CLOSE_TIME = get_date(float(msg['k']['t']))
                            logging.debug("BUY_CLOSE_TIME :: {0}".format(BUY_CLOSE_TIME))

                            MINIMUM_SPOT_PRICE = round(BUY_SPOT_PRICE * 0.997,4)
                            logging.debug("MINIMUM_SPOT_PRICE :: {0}".format(MINIMUM_SPOT_PRICE))

                            EXPECTED_SELL_SPOT_PRICE = round((1 + (TAKE_PROFIT+SELL_COMMISSION)/100) * PRICE_PAID/(QUANTITY-1),4)
                            logging.debug("EXPECTED_SELL_SPOT_PRICE :: {0}".format(EXPECTED_SELL_SPOT_PRICE))



                            back_tested['QUANTITY'].append(QUANTITY)
                            back_tested['BUY_CLOSE_TIME'].append(BUY_CLOSE_TIME)
                            back_tested['BUY_SPOT_PRICE'].append(BUY_SPOT_PRICE)
                            back_tested['PRICE_PAID'].append(PRICE_PAID)
                            back_tested['INIT_CASH'].append(INIT_CASH)

                            with open(order_csv_name,'a') as f:
                                writer = csv.writer(f)
                                writer.writerow([BUY_ORDER_ID,'buy'])


                            if order_status=="FILLED":
                                in_position = True
                                QUANTITY = float(buy_output['executedQty'])
                                logging.debug("New Buy Quantity :: {0}".format(QUANTITY))

                                logging.debug("Sleeping for 20 Sec")
                                time.sleep(20)
                                logging.debug("Waking after 20 Sec")

                            elif order_status=="PARTIALLY_FILLED":
                                in_position = True
                                QUANTITY = buy_output['executedQty']
                                logging.debug("Went into PARTIALLY_FILLED, cancelling the order")
                                partial_output = client.cancel_order(symbol=TRADE_SYMBOL,orderId = order_id)
                                logging.debug("Canceled partial order :: {0}".format(partial_output))

                            elif order_status == 'EXPIRED':
                                in_position = False
                                logging.debug("Order Got EXPIRED")
                            else:
                                logging.debug("Went into else below partial")
                                pass


                        except Exception as e:
                            logging.error('ERROR Occurred : {0}'.format(e.message))
                    else:
                        logging.debug('Min qty requirement not met')







                else:
                    logging.debug("Went into else of sma buying")
                    pass
        else:
            logging.error('ERROR OrderID Comdition Not met')
            pass
    else:
        logging.error('ERROR min_notional not met')
        pass


bm = BinanceSocketManager(client)
bm.start_kline_socket(TRADE_SYMBOL,process_message)
bm.start()
