import psycopg2
import pandas as pd
from datetime import datetime


##variables
now = str(datetime.now()) ##2021-04-26 01:59:00.000000
now_datetime = datetime.strptime(now[:-7], '%Y-%m-%d %H:%M:%S')

day0_midnight = '2021-04-26 00:00:00.000000' ##2021-04-26 00:00:00.000000
today0_midnight_datetime = datetime.strptime(day0_midnight[:-7], '%Y-%m-%d %H:%M:%S')
file_path = f'competition_ranking_{now}.csv'
id_file_name = 'id_list.csv'

conn = psycopg2.connect(
    host="",
    database="",
    user="",
    password="")

cur = conn.cursor()

id_source = pd.read_csv(id_file_name, header=0)
id_list = [x for x in id_source['user_id']]

##mark price
cur.execute(f'select to_timestamp("TS"/1000),* from "tbl_MarkPrices" where "ContractName"= \'BTCUSD\' and to_timestamp("TS"/1000) = \'{day0_midnight}\'')
mark_price_result = cur.fetchall()
mark_price = mark_price_result[0][-1]
print(f'mark_price_day0: {mark_price}')

##mark price_now
cur.execute(f'select to_timestamp("TS"/1000),* from "tbl_MarkPrices" where "ContractName"= \'BTCUSD\' and to_timestamp("TS"/1000) <= \'{now}\' order by "TS" desc limit 1')
mark_price_now_result = cur.fetchone()
mark_price_now = mark_price_now_result[-1]
print(f'mark_price_now: {mark_price_now}')


##Main Wallet
sql = f'select * from (select row_number() over (partition by "CID" order by "DateTime" desc) "num", * from "tbl_AccountMasters" where "DateTime" <= \'{day0_midnight}\' and  "Currency" = \'BTC\' and  "CID" in %(idList)s) wallet where wallet.num =1'
cur.execute(sql, { 'idList': tuple(id_list), # Converts the list to a tuple.
})
customers_main_wallet_btc_balance = cur.fetchall() ##-2 balance, 2 user_id

##Main Wallet_now
sql = f'select * from (select row_number() over (partition by "CID" order by "DateTime" desc) "num", * from "tbl_AccountMasters" where "DateTime" <= \'{now}\' and  "Currency" = \'BTC\' and  "CID" in %(idList)s) wallet where wallet.num =1'
cur.execute(sql, { 'idList': tuple(id_list), # Converts the list to a tuple.
})
customers_main_wallet_btc_balance_now = cur.fetchall() ##-2 balance, 2 user_id


# positions
# positions last updated time
sql = 'select * from(select "u_PNL","closed", row_number() over (partition by "user_id" order by "updatedAt" desc) as "num",to_timestamp("updatedAt") "update_time", "avg_entry_price","size", "user_id" from "tbl_Positions" where "user_id" in %(idList)s and symbol = \'BTCUSD\')source where source.num = 1'
cur.execute(sql, { 'idList': tuple(id_list), # Converts the list to a tuple.
})
customers_position_details = cur.fetchall()

##deposit after competition
sql = f'select "UserID", sum("Amount") from (select "UserID", "Amount"  from "tbl_Crypto_Deposits" where "Ticker" = \'BTC\' and to_timestamp("ConfirmedOnTS"/1000) > \'{day0_midnight}\' union all select "UserID", -1*"Amount" as "Amount" from "tbl_Crypto_Withdrawals" where "Ticker" = \'BTC\' and to_timestamp("ConfirmedOnTS"/1000) > \'{day0_midnight}\')dw where "UserID" in %(idList)s group by "UserID"'
cur.execute(sql, { 'idList': tuple(id_list), # Converts the list to a tuple.
})
customers_deposit = cur.fetchall()

##turnover
sql_for_turnover = f'select "ID", sum("Quantity") from (select "BuyerID" as "ID", sum("Quantity") "Quantity" from "tbl_Trades" where "BuyerID" in %(idList)s and "ContractName" = \'BTCUSD\' and to_timestamp("TradeTS") > \'{day0_midnight}\' group by "BuyerID" union all select "SellerID" as "ID", sum("Quantity") "Quantity" from "tbl_Trades" where "SellerID" in  %(idList)s and "ContractName" = \'BTCUSD\' and to_timestamp("TradeTS") > \'{day0_midnight}\' group by "SellerID") trade group by "ID"'
cur.execute(sql_for_turnover, { 'idList': tuple(id_list),
})
customers_turnover = cur.fetchall() ##0 id, 1 turnover

all_customers = []
for customer in customers_position_details:
    individual = []
    update_time = datetime.strptime(str(customer[3])[:-6], '%Y-%m-%d %H:%M:%S')
    closed = customer[1]
    size = customer[-2]
    user_id = customer[-1]
    avg_entry_price = customer[-3]
    upnl_now = customer[0]
    deposit = 0
    balance = 0
    turnover = 0

    if len(customers_turnover) > 0:
        for individual_turnover in customers_turnover:
            if individual_turnover[0] == user_id:
                turnover = individual_turnover[1]

    if len(customers_main_wallet_btc_balance) > 0:
        for x in customers_main_wallet_btc_balance:
            if x[2] == user_id:
                balance = x[-2]

    if len(customers_deposit) > 0:
        for deposit_amount in customers_deposit:
            if deposit_amount[0] == user_id:
                deposit = deposit_amount[1]


    if closed:
        size = 0
        upnl = customer[0]
    else:
        if size > 0:
            upnl = round(size * (1 / avg_entry_price - 1 / mark_price), 8)
        else:
            upnl = -1 * round(size * (1 / avg_entry_price - 1 / mark_price), 8)

    balace_plus_upnl = balance + upnl

##############now#######################################

    closed_now = customer[1]
    size_now = customer[-2]

    avg_entry_price_now = customer[-3]
    balance_now = 0
    if len(customers_main_wallet_btc_balance_now) > 0:
        for x in customers_main_wallet_btc_balance_now:
            if x[2] == user_id:
                balance_now = x[-2]

    if closed_now:
        size_now = 0
    else:
        if size_now > 0:
            upnl_now = round(size_now * (1 / avg_entry_price_now - 1 / mark_price_now), 8)
        else:
            upnl_now = -1 * round(size_now * (1 / avg_entry_price_now - 1 / mark_price_now), 8)
    # print(f'balance_now + upnl_now: {balance_now}, {upnl_now}')
    balace_plus_upnl_now = balance_now + upnl_now

####################cases
    if update_time < today0_midnight_datetime:
        if closed:
            size = 0
            upnl = customer[0]
        elif size > 0:
            upnl = round(size*(1/avg_entry_price - 1/mark_price), 8)
        else:
            upnl = -1 * round(size * (1 / avg_entry_price - 1 / mark_price), 8)
        balace_plus_upnl = balance + upnl

        individual.append(user_id)
        individual.append(size_now)
        individual.append(avg_entry_price_now)
        individual.append(mark_price_now)
        individual.append(upnl_now)
        individual.append(balance_now)
        individual.append(balace_plus_upnl)
        individual.append(balace_plus_upnl_now)
        # print(f'id: {user_id}, balance: {balance}, upnl: {upnl}')
        print(f'user_id: {user_id}, balace_plus_upnl_now:{balace_plus_upnl_now}, balace_plus_upnl:{balace_plus_upnl}, deposit:{deposit}, balace_plus_upnl + deposit:{balace_plus_upnl + deposit}')
        percentage_change = str(round((balace_plus_upnl_now - balace_plus_upnl - deposit)/(balace_plus_upnl + deposit) * 100, 8))+'%'
        individual.append(percentage_change)
        individual.append(turnover)

        all_customers.append(individual)

    else:
        ##find position id of trade right after 00:00, 6 position_id
        cur.execute(f'select to_timestamp("updatedAt"),"closed","orders",* from "tbl_Positions" where "symbol" = \'BTCUSD\' and user_id = \'{user_id}\' and to_timestamp("updatedAt") > \'{day0_midnight}\' order by "updatedAt" asc limit 1;')
        position_after_midnight = cur.fetchone()
        position_id = position_after_midnight[8]
        closed = position_after_midnight[1]


        ##find trades records after 00:00
        cur.execute(f'select * from "tbl_Position_Trades" where "PositionID" = \'{position_id}\' and to_timestamp("HighResTradeTS"/1000000000) > \'{day0_midnight}\' order by "HighResTradeTS" asc limit 1')
        trades_after_midnight = cur.fetchone()
        # print(f'id: {user_id}, updatetime: {update_time}')
        # print(f'trades_after_midnight: {trades_after_midnight}')

        ##not lquidate,
        if not closed and trades_after_midnight is not None:
            size = trades_after_midnight[3]
            avg_entry_price = trades_after_midnight[-3]


        ##liquidated and no trades after 00:00
        if closed and size != 0 and trades_after_midnight is None:
            # print(f'liquidate: id{user_id} ')
            size = -1 * size



        ## liquidated and trades after 00:00
        if closed and size != 0 and trades_after_midnight is not None:
            # print(f'liquidate: id{user_id}')
            ##find existing position before liquidation
            size = trades_after_midnight[3]
            avg_entry_price = trades_after_midnight[-3]


        if size > 0:
            upnl = round(size * (1 / avg_entry_price - 1 / mark_price), 8)
        else:
            upnl = -1 * round(size * (1 / avg_entry_price - 1 / mark_price), 8)

        balace_plus_upnl = balance + upnl

        individual.append(user_id)
        individual.append(size_now)
        individual.append(avg_entry_price_now)
        individual.append(mark_price_now)
        individual.append(upnl_now)
        individual.append(balance_now)
        individual.append(balace_plus_upnl)
        individual.append(balace_plus_upnl_now)
        print(f'user_id: {user_id}, balace_plus_upnl_now: {balace_plus_upnl_now}, balace_plus_upnl:{balace_plus_upnl}, deposit: {deposit}')
        percentage_change = str(round((balace_plus_upnl_now - balace_plus_upnl - deposit)/(balace_plus_upnl + deposit) * 100, 8))+'%'
        individual.append(percentage_change)
        individual.append(turnover)

        all_customers.append(individual)

df = pd.DataFrame(all_customers, columns=['user_id',
                                          'size',
                                          'avg_entry_price',
                                          'mark_price',
                                          'upnl',
                                          'balance',
                                          'balace_plus_upnl_day0',
                                          'balace_plus_upnl_now',
                                          'percentage_change',
                                          'turnover'
                                          ])
print(df)

df.to_csv(file_path)
print('csv done')


