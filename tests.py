import pandas as pd

df_top = pd.read_csv("coinpairByMarketPhase_BTC_1d.csv", usecols=['Coinpair'])
df_pos_1h = pd.read_csv("positions1h.csv")
df_pos_4h = pd.read_csv("positions4h.csv")
df_pos_1d = pd.read_csv("positions1d.csv")

# Rename the column to 'symbol'
df_top = df_top.rename(columns={'Coinpair': 'symbol'})

# Rename the 'symbol' column to 'Currency' in the 'df_pos1h', 'df_pos4h', and 'df_pos1d' dataframes
df_pos_1h = df_pos_1h.rename(columns={'Currency': 'symbol'})
df_pos_4h = df_pos_4h.rename(columns={'Currency': 'symbol'})
df_pos_1d = df_pos_1d.rename(columns={'Currency': 'symbol'})

# Filter the open positions
df_pos_1h = df_pos_1h.query('position == 1')[['symbol']]
df_pos_4h = df_pos_4h.query('position == 1')[['symbol']]
df_pos_1d = df_pos_1d.query('position == 1')[['symbol']]

# Merge the dataframes using an outer join on the 'symbol' column
merged_df = pd.merge(df_top, df_pos_1h, on='symbol', how='outer')
merged_df = pd.merge(merged_df, df_pos_4h, on='symbol', how='outer')
merged_df = pd.merge(merged_df, df_pos_1d, on='symbol', how='outer')

# Filter the rows where the 'symbol' column in the merged dataframe is null
new_symbols = merged_df.loc[merged_df['symbol'].isnull(), ['symbol_x', 'symbol_y', 'symbol']].stack().unique()

# Remove any null values from the list of new symbols
new_symbols = [symbol for symbol in new_symbols if pd.notnull(symbol)]

# Print the list of new symbols
print(new_symbols)