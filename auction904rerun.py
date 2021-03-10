import psycopg2
import psycopg2.extras
import pandas as pds
import math
from sqlalchemy import create_engine
import configparser

pds.option_context('display.max_rows', None, 'display.max_columns', None)

config = configparser.ConfigParser()
config.read("config.ini")

dbhost = config['DBCreds']['host']
dbport = config['DBCreds']['port']
dbname = config['DBCreds']['dbname']
dbuser = config['DBCreds']['user']
dbpass = config['DBCreds']['password']

con = psycopg2.connect(f"host={dbhost} port={dbport} dbname={dbname} user={dbuser} password={dbpass}")
alchemyEngine   = create_engine(f'postgresql+psycopg2://{dbuser}:{dbpass}@{dbhost}:{dbport}/{dbname}', pool_recycle=3600);


budget = 1600000000
clearing_price = 0
cleared = False

rounds = {
           '1': {'clock_pct': 180, 'budget': 0, 'clearing': False},
           '2': {'clock_pct': 170, 'budget': 0, 'clearing': False},
           '3': {'clock_pct': 160, 'budget': 0, 'clearing': False},
           '4': {'clock_pct': 150, 'budget': 0, 'clearing': False},
           '5': {'clock_pct': 140, 'budget': 0, 'clearing': False},
           '6': {'clock_pct': 130, 'budget': 0, 'clearing': False},
           '7': {'clock_pct': 120, 'budget': 0, 'clearing': False},
           '8': {'clock_pct': 110, 'budget': 0, 'clearing': False},
           '9': {'clock_pct': 100, 'budget': 0, 'clearing': False,},
           '10': {'clock_pct': 90, 'budget': 0, 'clearing': False},
           '11': {'clock_pct': 80, 'budget': 0, 'clearing': False},
           '12': {'clock_pct': 70, 'budget': 0, 'clearing': False},
           '13': {'clock_pct': 60, 'budget': 0, 'clearing': False},
           '14': {'clock_pct': 50, 'budget': 0, 'clearing': False},
           '15': {'clock_pct': 40, 'budget': 0, 'clearing': False},
           '16': {'clock_pct': 30, 'budget': 0, 'clearing': False},
           '17': {'clock_pct': 20, 'budget': 0, 'clearing': False},
           '18': {'clock_pct': 10, 'budget': 0, 'clearing': False},
           '19': {'clock_pct': 0, 'budget': 0, 'clearing': False}
          }

def census_block_groups():
    block_groups = {}
    cur = con.cursor(cursor_factory = psycopg2.extras.DictCursor)
    cur.execute("SELECT distinct census_id from auction904results")
    results = cur.fetchall()
    for row in results:
        block_groups[row[0]] = {'won': False}
    print("Number of census block groups", cur.rowcount)
    cur.close()
#    for row in block_groups:
#        print(row)
    # dbConnection = alchemyEngine.connect()
    # df_bg = pds.read_sql("SELECT distinct census_id, random() from auction904results  order by random() limit 1")
    # dbConnection.close()
    return block_groups


def get_round_results(round, block_group):
    # print(round, "block group: ", block_group)
    dbConnection    = alchemyEngine.connect()
    df = pds.read_sql(f"SELECT round, bidder, t_l_weight, price_point_bid, \
     bid_clock_pct_flag, min_scale_pct, my_assigned_status, not_assigned_reason, \
     biddable_next_round, substring(bid_id,19,1) as carryforward, selection_number, reserve_price \
     FROM auction904results r LEFT JOIN auction904eligible_blockgroups bg \
     on r.census_id = bg.block_group_id WHERE census_id = \
     '{block_group}' and round = '{round}' \
     and frn not in ('0004963088','0017434911','0026043968') \
     ", dbConnection )
    dbConnection.close()
    df['price_point_bid'] = df['price_point_bid'].astype(float)
    df['t_l_weight'] = df['t_l_weight'].astype(float)
    return df


def delete_records(round):
    print("about to delete the records for round :", round)
    cur = con.cursor(cursor_factory = psycopg2.extras.DictCursor)
    cur.execute(f"delete from auction904_rerun where round = '{round}'")
    con.commit()
    cur.close()


def calc_cost(round, price_point):
    cur = con.cursor(cursor_factory = psycopg2.extras.DictCursor)
    cur.execute(f"SELECT sum((({price_point} - t_l_weight::int) / 100::float) * reserve_price) as budget \
     FROM auction904_rerun r LEFT JOIN auction904eligible_blockgroups bg \
     on r.block_group = bg.block_group_id WHERE round = '{round}' and iteration = 4")
    results = cur.fetchall()
    for row in results:
        cost = int(row[0])
    print("The total cost for the round is: ", cost)
    rounds[round]["budget"] = cost
    return cost


        # delete_records(round)
def calc_clearing_price_point(auction_round, low_bid, high_bid, guess_bid_previous):
    global clearing_price
    guess_bid = round((((high_bid - low_bid) / 2) + low_bid), 2)
    print(guess_bid)
    if guess_bid == guess_bid_previous:
        print("done at :", guess_bid)
        clearing_price = guess_bid
        return guess_bid
    cost = calc_cost(auction_round, guess_bid)
    if cost < budget:
        calc_clearing_price_point(auction_round, guess_bid, high_bid, guess_bid)
    else:
        calc_clearing_price_point(auction_round, low_bid, guess_bid, guess_bid)

def write_results(round, block_group, status, bidder, price_point_bid, t_l_weight, assigned_support_price):
    sql = """INSERT INTO auction904_rerun (iteration, round, block_group, status, \
                  bidder, price_point_bid, t_l_weight, assigned_support_price)
                  VALUES (4, %s, %s, %s, %s, %s, %s, %s);"""
    # print(round, block_group, status, bidder, price_point_bid, t_l_weight)
    # print("about to write results")
    cur = con.cursor()
    cur.execute(sql, (round, block_group, status, bidder, price_point_bid, t_l_weight, assigned_support_price))
    con.commit()
    cur.close()


def calc_bid_stats(df, round):
    bid_stats = {}
    # bid_stats["bids"] = df
    # bid_stats["num_bids"] = len(df)
    bid_stats["low_weight"] = df['t_l_weight'].min()
    bid_stats["low_weights"] = df.loc[df['t_l_weight'] == bid_stats["low_weight"]]
    # bid_stats["low_weights_num"] = len(bid_stats["low_weights"])
    #
    bid_stats["low_bid"] = df['price_point_bid'].min()
    bid_stats["low_bids"] = df.loc[df['price_point_bid'] == bid_stats["low_bid"]]
    # bid_stats["low_bids_num"] = len(bid_stats["low_bids"])
    #
    bid_stats["low_bid_low_weight"] = bid_stats["low_weights"]['price_point_bid'].min()
    # bid_stats["low_bids_low_weight"] = df.loc[(df['t_l_weight'] == bid_stats["low_weight"]) & (df['price_point_bid'] == bid_stats["low_bid_low_weight"])]
    # bid_stats["low_bids_low_weight_num"] = len(bid_stats["low_bids_low_weight"])
    #
    bid_stats["low_weight_low_bid"] = bid_stats["low_bids"]['t_l_weight'].min()
    # bid_stats["low_weights_low_bid"] = df.loc[(df['price_point_bid'] == bid_stats["low_bid"]) & (df['t_l_weight'] == bid_stats["low_weight_low_bid"])]
    # bid_stats["low_weights_low_bid_num"] = len(bid_stats["low_weights_low_bid"])

    bid_stats["winner_order_later"] = df.sort_values(by=['t_l_weight','price_point_bid','selection_number'], ascending=[True, True, False]).iloc[0:8].index.tolist()
    bid_stats["winner_order_clearing"] = df.sort_values(by=['price_point_bid','t_l_weight','selection_number'], ascending=[True, True, False]).iloc[0:8].index.tolist()

    # df_clock = df.loc[df['price_point_bid'] == rounds[round]["clock_pct"]]
    # bid_stats["num_clock_bids"] = len(df_clock)
    # if bid_stats["num_clock_bids"] > 0:
    #     bid_stats["clock_low_weight"] = df_clock['t_l_weight'].min()
    #     bid_stats["clock_winning_order"] = df_clock.sort_values(by=['t_l_weight', 'selection_number'], ascending=[True, False]).iloc[0:5].index.tolist()
    #     bid_stats["clock_winner_index"] = bid_stats["clock_winning_order"][0]
    # else:
    #     pass
    #
    # df_cf = df.loc[df['carryforward'] == 'C']
    # bid_stats["num_cf_bids"] = len(df_cf)
    # if bid_stats["num_cf_bids"] > 0:
    #     bid_stats["cf_low_weight"] = df_cf['t_l_weight'].min()
    #     bid_stats["cf_winning_order"] = df_cf.sort_values(by=['t_l_weight', 'selection_number'], ascending=[True, False]).iloc[0:5].index.tolist()
    #     bid_stats["cf_winner_index"] = bid_stats["cf_winning_order"][0]
    # else:
    #     pass
    return bid_stats


def support_payment(df, round, i, winner_order_exclude):
    assigned_support_price = 0
    df_exclude = df.iloc[winner_order_exclude]
    if rounds[round]["clearing"] == True:
        df_exclude_lower = df_exclude.loc[df_exclude["price_point_bid"] <= df.loc[i]['price_point_bid']]
    else:
        df_exclude_lower = df_exclude.loc[df_exclude["t_l_weight"] <= df.loc[i]['t_l_weight']]
    # print("assigning support payment")
    # print(df)
    # print("record number: ", i)
    # print("number of bids excluding itself: ", len(df_exclude_lower))
    # print("this is the clearing round: ", rounds[round]["clearing"])
    # print("budget has cleared: ", cleared)
    if len(df_exclude_lower) == 0 and rounds[round]["clearing"] == True:
        assigned_support_price = clearing_price
        # print("assigned1: ", assigned_support_price)
    elif len(df_exclude_lower) > 0 and rounds[round]["clearing"] == True:
        assigned_support_price = max(df.loc[i]['price_point_bid'], df_exclude_lower["price_point_bid"].min())
        # print("assigned2: ", assigned_support_price)
    elif len(df_exclude_lower) == 0 and cleared == True:
        assigned_support_price = rounds[str(int(round) - 1)]["clock_pct"]
        # print("assigned3: ", assigned_support_price)
    elif len(df_exclude_lower) > 0 and cleared == True:
        assigned_support_price = max(df.loc[i]['price_point_bid'], df_exclude_lower["price_point_bid"].min())
        # print("assigned4: ", assigned_support_price)
    else:
        pass
    return assigned_support_price


def winner(df, round, block_group, bid_stats):
    if rounds[round]["clearing"] == True:
        winner_order = bid_stats["winner_order_clearing"]
    elif cleared == True:
        winner_order = bid_stats["winner_order_later"]
    won = False
    # print(df)
    # print("winning order: ", winner_order)
    for i in winner_order:
        winner_order_exclude = list(filter(lambda num: num != i,
                                            winner_order))
        bid_stats_exclude = calc_bid_stats(df.iloc[winner_order_exclude], round)

        if bid_stats_exclude["low_bid"] == rounds[round]["clock_pct"] and bid_stats_exclude["low_weight_low_bid"] <= df.loc[i]["t_l_weight"]:
            assignable = False
        else:
            assignable = True

        if df.loc[i]["not_assigned_reason"] == "Minimum scale percentage not met":
            pass
        elif assignable == True:
            won = True
            # print("block group won: ", block_group)
            block_groups[block_group]["won"] = True
            # print("winning record is: ", i, " which is ", df.loc[i]["bidder"])
            # print(df)
            assigned_support_price = support_payment(df, round, i, winner_order_exclude)
            block_groups[block_group]["won"] = True
            write_results(round, block_group, 'won', df.loc[i]["bidder"], df.loc[i]['price_point_bid'], df.loc[i]['t_l_weight'], assigned_support_price)
            break


    if won == False:
        write_results(round, block_group, 'push', 'NA', bid_stats["low_bid"], bid_stats["low_weight_low_bid"], 0)


def iterate_bgs(round):
    print("about to process results for round :", round)
    print("budget has cleared: ", cleared)
    for block_group in block_groups:
        if block_groups[block_group]["won"] == True:
            pass
        else:
            df = get_round_results(round, block_group)
            # bid_stats_eligible = calc_bid_stats(df.loc[df['not_assigned_reason'] != 'Minimum scale percentage not met'], round)
            if len(df) > 0:
                if cleared == False:
                    write_results(round, block_group, 'push', 'UNK', rounds[round]["clock_pct"], df['t_l_weight'].min(), 0)
                else:
                    bid_stats = calc_bid_stats(df, round)
                    winner(df, round, block_group, bid_stats)#, bid_stats_eligible)
            else:
                pass

def sort_results(round):
    global cleared
    global clearing_price
    print("round: ", round, " clock pct:", rounds[round]["clock_pct"])
    iterate_bgs(round)
    cost = calc_cost(round, rounds[round]["clock_pct"])
    if cleared == False and cost < budget:
        print("This is the clearing round")
        cleared = True
        rounds[round]["clearing"] = True
        clearing_price = calc_clearing_price_point(round, rounds[round]["clock_pct"], rounds[str(int(round) - 1)]["clock_pct"], rounds[round]["clock_pct"])
        print("The clearing price for the round is: ", clearing_price)
        delete_records(round)
        iterate_bgs(round)


def iterate_rounds():
    for round in rounds:
        sort_results(round)

block_groups = census_block_groups()
#sort_results('14')
iterate_rounds()

#450750107001
