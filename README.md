# BEC - Bot-EMA-Cross
Trading bot for Binance exchange using EMA cross strategy with Telegram integration and web dashboard.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=jptsantossilva/BEC&type=Date)](https://star-history.com/#jptsantossilva/BEC&Date)

## Installation
[Installation instructions](https://docs.google.com/document/d/1ERtxjcdrznMWXragmBh5ZimIn6_PGn2sde0j_x4CktA/edit?usp=sharing)

## Updates
Follow the last updates from the change log [here](https://github.com/jptsantossilva/BEC/blob/main/CHANGELOG.md)

## Backtest results
Check [here](https://github.com/jptsantossilva/BEC/blob/main/coinpairBestEma%20Full%20List.csv) the returns in % using last 4 years of backtest data. Results from 9th january 2023.

## Features
- Runs 1D, 4H and 1H time frames.
- Trade against stables pairs (BUSD or USDT) or against BTC.
- Automatically chooses, on a daily basis, coins that are in accumulation phase (price close>DSMA200>DSMA50) or bullish phase (price close>DSMA50>DSMA200).
- For those coins in accumulation and bullish phases, calculates EMA cross combination (with [backtesting python library](https://kernc.github.io/backtesting.py)) with highest returns for 1D, 4H and 1H time frames. 4-years of historicals prices are used in backtesting. 
- Each coin will be traded with its best EMA cross on each timeframe. 
- If best EMA result is negative the coin will be ignored and will not be traded. 
- To calculate Best EMA against stable pairs uses the stable pair (BUSD or USDT) with more historical data.
- Web dashboard with realized and unrealized PnL, blacklist, Top Performers, Best EMAs,.
- Telegram message notifications - every time bot is executed; open position; close position; position status summary; coins in accumulation and bullish market phases
- Blacklist - coins to be ignored. Bot will not trade them.

![dashboard](https://raw.githubusercontent.com/jptsantossilva/BEC/main/docs/dashboard.png)
![upnl](https://raw.githubusercontent.com/jptsantossilva/BEC/main/docs/upnl.png)
![rpnl](https://raw.githubusercontent.com/jptsantossilva/BEC/main/docs/rpnl.png)
![signals](https://raw.githubusercontent.com/jptsantossilva/BEC/main/docs/signals.png)
![top_perf](https://raw.githubusercontent.com/jptsantossilva/BEC/main/docs/top_perf.png)
![blacklist](https://raw.githubusercontent.com/jptsantossilva/BEC/main/docs/blacklist.png)
![bema](https://raw.githubusercontent.com/jptsantossilva/BEC/main/docs/bema.png)
![settings](https://raw.githubusercontent.com/jptsantossilva/BEC/main/docs/settings.png)
## Disclaimer
This software is for educational purposes only. Use the software at **your own risk**. The authors and all affiliates assume **no responsibility for your trading results**. **Do not risk money that you are afraid to lose**. There might be **bugs** in the code. This software does not come with **any warranty**.

## 📝 License

This project is [MIT](https://github.com/jptsantossilva/BEC/blob/main/LICENSE.md) licensed.

Copyright © 2023 [João Silva](https://github.com/jptsantossilva)




