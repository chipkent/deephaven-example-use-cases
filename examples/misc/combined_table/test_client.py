
# A script to test combined_table on the client

from combined_table_client import combined_table
from deephaven_enterprise.client.session_manager import SessionManager

# For connection details, see: https://deephaven.io/enterprise/docs/coreplus/coreplus-python-client/

connection_info = "https://hostname:8123/iris/connection.json"
session_mgr: SessionManager = SessionManager(connection_info)

# Authenticate
session_mgr.password("username", "password")
# -- or --
# session_mgr.private_key("/path-to-private-key/priv-username.base64.txt")

session = session_mgr.connect_to_new_worker(name=None, heap_size_gb=4.0)

# Begin testing
ct = combined_table(session, "FeedOS", "EquityQuoteL1")
ct_live = ct.live
ct_hist = ct.historical
ct_comb = ct.combined

# test apply
cta = ct.apply
cta_live = cta.live
cta_hist = cta.historical
cta_comb = cta.combined

# test unspecified method routing
print(ct.is_refreshing)

# Test a where without applying
f1 = ct.where("Date > `2024-04-09`", apply=False)
fc1 = f1.combined
h1 = f1.head(3)

# Test another where with applying (historical and live already computed)
f2 = f1.where("Date < `2024-04-11`")
fc2 = f2.combined
h2 = f2.head(3)

# Test another where with applying (historical and live not already computed)
f2a = ct.where("Date > `2024-04-09`", apply=False).where("Date < `2024-04-11`")
fc2a = f2a.combined
h2a = f2a.head(3)

# Test a where on an applied table
f3 = f2.where("Date > `2024-04-01`")
fc3 = f3.combined
h3 = f3.head(3)

# Test where with no filters
f4 = f3.where()
fc4 = f4.combined
h4 = f4.head(3)
assert(f4 == f3)

# Test where_in with a table
tf = session.empty_table(1).update("Date=`2024-04-09`")
f5 = f1.where_in(tf, "Date")
fc5 = f5.combined
h5 = f5.head(3)

# Test where_in with a CombinedTable
f6 = f1.where_in(f3, "Date")
fc6 = f6.combined
h6 = f6.head(3)

# Test where_not_in with a table
tf = session.empty_table(1).update("Date=`2024-04-09`")
f7 = f1.where_not_in(tf, "Date")
fc7 = f7.combined
h7 = f7.head(3)

# Test where_in with a CombinedTable
f8 = f1.where_not_in(f3, "Date")
fc8 = f8.combined
h8 = f8.head(3)

# Not yet supported
# # Test where_one_of
# f9 = f1.where_one_of(["Date=`2024-04-09`", "Date=`2024-04-06`"])
# fc9 = f9.combined
# h9 = f9.head(3)

# Test materializing input CombinedTables
f10 = f2.natural_join(f2, ["Date", "Timestamp"], ["JAsk=Ask"])

# Test combined join
f11 = f2.combined_join("natural_join", f2, ["Date", "Timestamp"], ["JAsk=Ask"])
f12 = f2.combined_join("natural_join", f2.historical, ["Date", "Timestamp"], ["JAsk=Ask"])