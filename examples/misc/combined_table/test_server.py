
# A script to test combined_table on the server

from combined_table_server import combined_table

ct = combined_table("FeedOS", "EquityQuoteL1")
ct_live = ct.live
ct_hist = ct.historical
ct_comb = ct.combined
# print(ct.is_replay) # Method does not exist
print(ct.is_blink)
f1 = ct.where("Date > `2024-04-09`")
fc1 = f1.combined
h1 = f1.head(3)

f2 = f1.where("Date < `2024-04-11`")
fc2 = f2.combined
h2 = f2.head(3)
