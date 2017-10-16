#
# vehicles.py
# @author Sidharth Mishra
# @description MongoDB script
# @created Sun Oct 15 2017 14:58:59 GMT-0700 (PDT)
# @last-modified Sun Oct 15 2017 15:16:21 GMT-0700 (PDT)
#

"""
This is a monogdb script for updating Quotes after referring to the VehicleMasters,
and VehicleCatalogs collections.
"""

from pymongo import MongoClient
from copy import deepcopy


# mongoClient gets the MongoClient for the given hostname and port
mongoClient = lambda hostname, port: MongoClient(host=hostname, port=port)

# gets the database name
db = lambda client, name: client.get_database(name)

# Quotes -- collection
# Gets the Quotes collection
Quotes = lambda hostname, port, dbName: db(mongoClient(hostname, port), dbName).get_collection("Quotes")

# VehicleMasters -- collection
# Gets the VehicleMasters collection
VehicleMasters = lambda hostname, port, dbName: db(mongoClient(hostname, port), dbName).get_collection("VehicleMasters")

# VehicleCatalogs -- collection
# Gets the VehicleCatalogs collection
VehicleCatalogs = lambda hostname, port, dbName: db(mongoClient(hostname, port), dbName).get_collection("VehicleCatalogs")

# NewQuotes -- collection
# Collection contains the bare minimum new Quote information, used as staging
# Gets the handle to the NewQuotes collection
NewQuotes = lambda hostname, port, dbName: db(mongoClient(hostname, port), dbName).get_collection("NewQuotes")

# UQuotesLog -- collection
# Collection contains the updation logs
# Ges the handle to the UQuotesLog collection
UQuotesLog = lambda hostname, port, dbName: db(mongoClient(hostname, port), dbName).get_collection("UQuotesLog")


# Global list for holding all the active vehicle masters
def getVehicleMastersForWI(hostname, port, dbName):
  '''
  Fetches the `Active` VehicleMaster documents for the state of `WI`
  '''
  activeVehicleMasters = []
  cursor = VehicleMasters(hostname, port, dbName).aggregate(
    [
      {
        "$unwind": "$states"
      },
      {
        "$match": {
          "status": "Active",
          "states": "WI"
        }
      },
      {
        "$project": {
          "_id": 1
        }
      }
    ],
    allowDiskUse=True
  )
  for master in cursor:
    activeVehicleMasters.append(master)
  return activeVehicleMasters

# Fetch legacy quotes for updation
def getLegacyQuotesForUpdation(hostname, port, dbName):
  '''
  Fetches the legacyQuotes from the Quotes collection for the 
  given hostname, port and dbName.
  '''
  legacyQuotes = []
  cursor = Quotes(hostname, port, dbName).aggregate(
    [
      {
        "$unwind": "$legacyQuotes"
      },
      {
        "$match": {
          "legacyQuotes.sourceSystem": 1
        }
      },
      {
        "$unwind": "$legacyQuotes.assets"
      },
      {
        "$project": {
          "legacyQuotes._id": 1,
          "legacyQuotes.assets._id": 1,
          "legacyQuotes.assets.year": 1,
          "legacyQuotes.assets.make": 1,
          "legacyQuotes.assets.model": 1,
          "legacyQuotes.assets.bodyStyle": 1,
        }
      }
    ],
    allowDiskUse=True
  )
  for quote in cursor:
    legacyQuotes.append(quote)
  return legacyQuotes

# Fetch the matching vehicle catalogs
def getMatchingVehicleCatalogs(hostname, port, dbName, versionId, year, make, model, bodyStyle):
  '''
  Fetches the matching Vehicle catalogs from the VehicleCatalogs collection for 
  the given hostname, port, and dbName.
  '''
  matchingVehicleCatalogs = []
  cursor = VehicleCatalogs(hostname, port, dbName).aggregate(
    [
      {
        "$match": {
          "versionId": versionId,
          "year": year
        }
      },
      {
        "$unwind": "$makes"
      },
      {
        "$match": {
          "makes.key": str(make)
        }
      },
      {
        "$unwind": "$models"
      },
      {
        "$unwind": "$models.models"
      },
      {
        "$match": {
          "models.models.key": str(model)
        }
      },
      {
        "$match": {
          "bodyStyles.bodyStyles.key": str(bodyStyle)
        }
      },
      {
        "$project": {
          "makes_id": 1,
          "makes.key": 1,
          "models._id": 1,
          "models.models._id": 1,
          "models.models.key": 1,
          "bodyStyle": {
            "$filter": {
              "input": "$bodyStyles",
              "as": "bs",
              "cond": {
                "$and": [
                  {
                    "$eq": [
                      "$$bs._id",
                      "$models.models._id"
                    ]
                  },
                  {
                    "$eq": [
                      "$makes._id",
                      "$models._id"
                    ]
                  }
                ]
              }
            }
          },
          "_id":0
        }
      }
    ],
    allowDiskUse=True
  )
  for catalog in cursor:
    matchingVehicleCatalogs.append(catalog)
  return matchingVehicleCatalogs



### Stages of the operation

# stage#1
def stage1():
  '''
  First stage of the operation, gets the oldQuotes to update.

  This stage returns the list of oldQuotes that needs to be updated.
  '''
  oldQuotes = []
  quotes = getLegacyQuotesForUpdation(
    "E2LXMONGDBA02.west.esurance.com", 27017, "cache")
  for quote in quotes:
    quoteId = quote["_id"]
    legacyQuoteId = quote["legacyQuotes"]["_id"]
    assetId = quote["legacyQuotes"]["assets"]["_id"]
    year = quote["legacyQuotes"]["assets"]["year"]
    make = quote["legacyQuotes"]["assets"]["make"]
    model = quote["legacyQuotes"]["assets"]["model"]
    bodyStyle = quote["legacyQuotes"]["assets"]["bodyStyle"]
    if (year is not None and 
      make is not None and 
      model is not None and
      bodyStyle is not None):
      oldQuotes.append(quote)
    else:
      print "The quote with quoteId :: ", quoteId, " doesn't have legacyQuotes field"
  return oldQuotes

# stage#2
def stage2(oldQuotes):
  '''
  Second stage, each oldQuote obtained from stage1, a newQuote is
  constructed replacing the make, model, and bodyStyle keys with their
  IDs. This stage creates new Collections named `NewQuotes` and `UQuotesLog`.
  `NewQuotes` contains the newUpdated bare minimum quotes.
  `UQuotesLog` contains the diff.

  This stage returns True upon successful execution, else returns False.
  '''
  activeVehicleMasters = getVehicleMastersForWI(
    "E2LXMONGDBA02.west.esurance.com", 27017, "cache")
  activeVersionId = activeVehicleMasters[0]["_id"]
  for quote in oldQuotes:
    quoteId = quote["_id"];
    legacyQuoteId = quote["legacyQuotes"]["_id"];
    assetId = quote["legacyQuotes"]["assets"]["_id"];
    year = quote["legacyQuotes"]["assets"]["year"];
    make = quote["legacyQuotes"]["assets"]["make"] + 0;
    model = quote["legacyQuotes"]["assets"]["model"] + 0;
    bodyStyle = quote["legacyQuotes"]["assets"]["bodyStyle"] + 0;
    catalogs = getMatchingVehicleCatalogs(
      "E2LXMONGDBA02.west.esurance.com",
      27017, 
      "cache", 
      activeVersionId, 
      year, 
      make, 
      model, 
      bodyStyle
    )
    for catalog in catalogs:
      makeId = catalog["makes"]["_id"];
      modelId = catalog["models"]["models"]["_id"];
      bodyStyleId = catalog["bodyStyle"][0]["bodyStyles"][0]["_id"];
      newQuote = deepcopy(quote)
      uQuote = deepcopy(quote)
      # for NewQuotes
      newQuote["legacyQuotes"]["assets"]["make"] = makeId;
      newQuote["legacyQuotes"]["assets"]["model"] = modelId;
      newQuote["legacyQuotes"]["assets"]["bodyStyle"] = bodyStyleId;
      # for UQuotesLog
      uQuote["legacyQuotes"]["assets"]["makeOld"] = make;
      uQuote["legacyQuotes"]["assets"]["make"] = makeId;
      uQuote["legacyQuotes"]["assets"]["modelOld"] = model;
      uQuote["legacyQuotes"]["assets"]["model"] = modelId;
      uQuote["legacyQuotes"]["assets"]["bodyStyleOld"] = bodyStyle;
      uQuote["legacyQuotes"]["assets"]["bodyStyle"] = bodyStyleId;
      # insert the newQuote into NewQuotes collection
      NewQuotes(
        "E2LXMONGDBA02.west.esurance.com", 
        27017, 
        "cache"
      ).insert_one(newQuote)
      # insert the uQuote into UQuotesLog collection
      UQuotesLog(
        "E2LXMONGDBA02.west.esurance.com", 
        27017, 
        "cache"
      ).insert_one(uQuote)
  else:
    return False
  return True

# stage#3
def stage3(stage2Status):
  '''
  :Consumer: This is the consumer/terminal operation in the pipeline.
  This stage will fail if stage#2 fails.
  This stage updates the actual quotes in the `Quotes` collection by using the `NewQuotes` collection.
  '''
  if not stage2Status:
    print "stage#2 has failed so bailing out..."
    return
  newQuotes = NewQuotes(
    "E2LXMONGDBA02.west.esurance.com", 
    27017, 
    "cache"
  ).find()
  for newQuote in newQuotes:
    Quotes(
      "E2LXMONGDBA02.west.esurance.com", 
      27017, 
      "cache"
    ).update_one(
      {
        
      },
      {},
      upsert=False
    )

  else:
    print "No new quotes in the NewQuotes collection, bailing out..."
    return False


