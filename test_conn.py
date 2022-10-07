from azure_connector import AzureConn
dbc = AzureConn("sg_property")
df = dbc.query('select * from transactions limit 10')
# use non pandas functions to read or change to sqlalchemy
print('done')