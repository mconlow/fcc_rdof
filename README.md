# fcc_rdof
Rerun the FCC Rural Digital Opportunity Fund Auction

**Setup**

We assume you have the FCC RDOF Auction 904 results loaded into a Postgres database. Results are available [here](https://auctiondata.fcc.gov/public/projects/auction904/static_files/all_bid_results.zip).

Then we need a table to store the results of our auction re-runs. That can be created:

    create table auction904_rerun (
    id INT GENERATED ALWAYS AS IDENTITY,
    iteration integer,
    round integer, 
    block_group varchar(15),
    status varchar(255),
    bidder varchar(255), 
    price_point_bid float, 
    t_l_weight integer);

Create a config.ini file in the same directory where you'll run this program. The config.ini file should take the form:

    [DBCreds]
    host: <hostname of db>
    port: <port>
    dbname: <db username>
    user: <db username>
    password: <password>

Create a virtual enviornment to run the python program:

    python3 -m venv venv

Then activate the virtual env:

    source venv/bin/activate

Install the dependencies:

    pip3 install -r requirements.txt

**Running the program**

You should now be able to run the program with:

    python3 auction904rerun.py

It can take a couple hours to run the auction from round 1
