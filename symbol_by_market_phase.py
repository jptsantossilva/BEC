from bec.symbol_by_market_phase import *

if __name__ == "__main__":
    time_frame, trade_against_value = read_arguments()
    main(timeframe=time_frame)
