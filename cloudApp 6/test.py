from sshtunnel import SSHTunnelForwarder
from bson import ObjectId
import pymongo
import pprint
from bson.code import Code
from bson.son import SON

MONGO_HOST = "devincimdb2017.westeurope.cloudapp.azure.com"
MONGO_DB = "movieLens"
MONGO_USER = "administrateur"
MONGO_PASS = "V8eOFR%_"

server = SSHTunnelForwarder(
    (MONGO_HOST,22),
    ssh_username=MONGO_USER,
    ssh_password=MONGO_PASS,
    remote_bind_address=('10.0.0.94', 30000)
)
server.start()

print(server.local_bind_port)  # show assigned local port
# work with `SECRET SERVICE` through `server.local_bind_port`.



client = pymongo.MongoClient('localhost', server.local_bind_port)
db = client.movieLens
collection = db.movie
print("Connected")
# close ssh tunnel

pipeline = [
            {"$match": {"user.userid": 2955}},
            {"$unwind": "$user"},
            {"$group": {"_id": "$movieid", "average": {"$avg": "$user.rating"}, "tot": {"$sum": 1}}},
            {"$match": {"tot": {"$gt": 1000}}},
            {"$sort": SON([("average", -1), ("sum", 1)])}]

rankinglist = list(collection.aggregate(pipeline))

print(rankinglist)

abc = collection.status()
print(abc)

client.close()
server.stop()