import os
import zlib

document="""
{"reason_to_disable":null,"installations":[],"disabled":false,"player_data":{},"game":"149","credentials":[{"is_verified":true,"verification_code":null,"type":"ghost","uid":"c7ad66fa-4c8c-11e5-bb3c-08002754a170"}],"aliases":[],"inactive_credentials":[{"is_verified":true,"verification_code":null,"type":"system","uid":"22222222"}],"client_ids":{"148:18154:1.1.1:ios":"2015-08-27 15:21:44Z"},"_meta":{"type":"account","service":"janus","size":605},"modified":"\"2015-08-27 15:25:23Z\"","last_device":null,"last_login":"2015-08-27 15:21:44Z","fed_id":"455b87ae-4c8c-11e5-91a2-08002754a170","permissions":[]}
"""
# print len(document)
import MySQLdb

db=MySQLdb.connect(user='root',passwd='1')
cursor=db.cursor()
doc=zlib.compress(document)
cursor.execute('update janus_148_51_data.data_account set document=%s where docid=%s',(doc,'148:account:455b87ae-4c8c-11e5-91a2-08002754a170'))
db.commit()
db.close()
# os.execl()