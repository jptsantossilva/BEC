# Binance-Trader-EMA-Cross
EMA-Cross fully automatic trading bot for Binance Spot with Telegram integration

## Installation
[Installation instructions](https://docs.google.com/document/d/1ERtxjcdrznMWXragmBh5ZimIn6_PGn2sde0j_x4CktA/edit?usp=sharing)


## Features
- Runs 1D, 4H and 1H timeframes independently.
- Automatically chooses, on a daily basis, trading coins that are in accumulation phase (price close>DSMA200>DSMA50) and bullish phase (price close>DSMA50>DSMA200).
- For those trading coins in accumulation and bullish phases, calculates EMA cross combination (with [backtesting python library](https://kernc.github.io/backtesting.py)) with highest returns for 1D, 4H and 1H timeframes. 4-years of historicals prices are used in backtesting. 
- Each coin will be traded with its best EMA cross on each timeframe. 
- If best EMA result is negative the coin will be ignored and will not be traded. 
- To calculate Best EMA uses the stable pair (BUSD or USDT) with more historical data.
- Uses csv files to store which coins are in position.
- Uses csv files to store executed buy and sell orders.
- Calculates PnL for executed sell orders.
- Telegram message notifications - every time bot is executed; open position; close position; position status summary; coins in accumulation and bullish market phases
- Blacklist - coins to be ignored. Bot will not trade them.

## 📝 License

This project is [MIT](https://github.com/jptsantossilva/Binance-Trader-EMA-Cross/blob/main/LICENSE.md) licensed.

Copyright © 2022 [João Silva](https://github.com/jptsantossilva)




