import pandas as pd

# Define a function to get the year and month from a datetime object
def get_year_month(date):
    return date.year, date.month

def calculate_pnl_closed_positions():

    # Get user input for the year and month
    print('Choose period for PnL analysis of closed positions')
    year = int(input('Enter year (YYYY): '))
    month = int(input('Enter month (MM): '))

    print('\nPnL - CLOSED POSITIONS')
    print('---------------------')
    
    positionsTimeframe = ["1d", "4h", "1h"] 

    results_df = pd.DataFrame(columns=['bot','Year','Month','pnl_%','pnl_usd','trades'])

    for tf in positionsTimeframe: 
        df = pd.read_csv('orders'+tf+'.csv')
    
        # Convert the time column to a Pandas datetime object
        df['time'] = pd.to_datetime(df['time'])
    
        # Filter the dataframe by the year and month provided by the user, and by the 'SELL' side
        df = df[(df['time'].dt.year == year) & (df['time'].dt.month == month) & (df['side'] == 'SELL')]

        df['bot'] = tf
        # Get the total number of rows in the filtered dataframe
        trades = len(df)

        # remove miliseconds
        df['time'] = df['time'].dt.strftime("%Y-%m-%d %H:%M:%S")
        month_df = df[['bot','time','symbol','executedQty','price','pnlusd','pnlperc']].copy()
        month_df = month_df.rename(columns={'executedQty':'quantity','price':'sellPrice','pnlusd':'pnl_usd','pnlperc':'pnl_%'})
        

        print('')              
        print(month_df)

        # Calculate the sums of the 'pnlperc' and 'pnlusd' columns
        pnl_perc_sum = month_df['pnl_%'].sum()
        pnl_usd_sum = round(month_df['pnl_usd'].sum(),2)

        # Create a new dataframe with the results
        df_new = pd.DataFrame({
                'bot': [tf],
                'Year': [year],
                'Month': [month],
                'pnl_%': [pnl_perc_sum],
                'pnl_usd': [pnl_usd_sum],
                'trades': [trades]})
        # append the new data to the existing DataFrame
        results_df = pd.concat([results_df, df_new], ignore_index=True)

    # Calculate the sum of values in pnl 
    sum_pnl_perc = results_df['pnl_%'].sum()
    sum_pnl_usd = results_df['pnl_usd'].sum()
    sum_trades = results_df['trades'].sum()
    # Add a new row at the end of the dataframe with the sum values
    results_df.loc[len(results_df)] = ['TOTAL','', '', sum_pnl_perc, sum_pnl_usd, sum_trades]
    
    return results_df

def calculate_pnl_open_positions():
    
    print('\nPnL - CURRENT OPEN POSITIONS')
    print('---------------------')

    positionsTimeframe = ["1d", "4h", "1h"] 

    results_df = pd.DataFrame(columns=['bot','pnl_%','pnl_usd','positions'])

    for tf in positionsTimeframe: 
        df = pd.read_csv('positions'+tf+'.csv')
    
        # Convert the time column to a Pandas datetime object
        # df['time'] = pd.to_datetime(df['time'])
    
        # Filter the dataframe by the year and month provided by the user, and by the 'SELL' side
        # month_df = df[(df['time'].dt.year == year) & (df['time'].dt.month == month) & (df['position'] == '1')]
        df = df[(df['position'] == 1)]

        # Get the total number of rows in the filtered dataframe
        positions = len(df)
        df['bot'] = tf

        df['pnlusd'] = (df['currentPrice']*df['quantity'])-(df['buyPrice']*df['quantity']) 
        # calc pnlperc2 to avoid the round from the original pnlperc
        df['pnlperc2'] = (((df['currentPrice']*df['quantity'])-(df['buyPrice']*df['quantity']))/(df['buyPrice']*df['quantity']))*100

        positions_df = df[['bot','Currency','quantity','buyPrice','pnlusd','pnlperc2']].copy()
        positions_df = positions_df.rename(columns={'Currency':'symbol','pnlusd':'pnl_usd','pnlperc2':'pnl_%'})

        # Calculate the sums of the 'pnlperc' and 'pnlusd' columns
        pnl_perc_sum = round(positions_df['pnl_%'].sum(),2)
        pnl_usd_sum = round(positions_df['pnl_usd'].sum(),2)

        print('')              
        print(positions_df)

        # Create a new dataframe with the results
        df_new = pd.DataFrame({
                'bot': [tf],
                'pnl_%': [pnl_perc_sum],
                'pnl_usd': [pnl_usd_sum],
                'positions': [positions]})
        # append the new data to the existing DataFrame
        results_df = pd.concat([results_df, df_new], ignore_index=True)

    # Calculate the sum of values in pnl 
    sum_pnl_perc = results_df['pnl_%'].sum()
    sum_pnl_usd = results_df['pnl_usd'].sum()
    sum_positions = results_df['positions'].sum()
    # Add a new row at the end of the dataframe with the sum values
    results_df.loc[len(results_df)] = ['TOTAL', sum_pnl_perc, sum_pnl_usd, sum_positions]
    
    return results_df

result = calculate_pnl_closed_positions()
print('\nTOTAL PnL - CLOSED POSITIONS')
print(result)

# print('\n----------------------------\n')

result = calculate_pnl_open_positions()
print('\nTOTAL PnL - CURRENT OPEN POSITIONS')
print('-------------------------------')
print(result)