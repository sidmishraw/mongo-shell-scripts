/**
* vehicles.js
* @author Sidharth Mishra
* @description MongoShell script for vehicles
* @created Fri Oct 13 2017 19:02:05 GMT-0700 (PDT)
* @copyright 2017 Sidharth Mishra
* @last-modified Fri Oct 13 2017 19:02:05 GMT-0700 (PDT)
*/

connection = new Mongo(); // open a new connection to the local monogdb instance
db = connection.getDB("admin"); // same as `use admin`
db.auth("root", "EsuDba2016#"); // for logging in

db = connection.getDB("cache"); // same as `use cache`

// collection names
var vehiclemasters = "VehicleMasters";
var quotes = "Quotes";
var vehiclecatalogs = "VehicleCatalogs";

/**
 * For fetching `versionId` from `VehicleMasters` collection
 * 
 * @returns {cursor} `VehicleMasters` cursor for traversing matching documents
 */
var getVehicleMasterForWI = function () {
  return db.getCollection(vehiclemasters).aggregate([
    {
      $unwind: "$states"
    },
    {
      $match: {
        "status": "Active",
        "states": "WI"
      }
    },
    {
      $project: {
        "_id": 1
      }
    }
  ]);
};

/**
 * Fetches the legacyQuotes for updation
 * 
 * @returns {cursor} The cursor for traversing `Quotes` documents
 */
var getLegacyQuotesForUpdation = function () {
  return db.getCollection(quotes).aggregate([
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
        "legacyQuotes.assets.bodyStyle": 1
      }
    }
  ]);
};

/**
 * Gets the matching vehicle catalog for the versionId, year, make, model and bodyStyle combination.
 * 
 * @param {String} versionId The versionId obtained from the `VehicleMasters`
 * @param {Number} year The year obtained from the `Quotes`
 * @param {Number} make The make obtained from the `Quotes`
 * @param {Number} model The model obtained from the `Quotes`
 * @param {Number} bodyStyle The bodyStyle obtained from the `Quotes`
 * 
 * @returns {cursor} The cursor for the documents in the `VehicleCatalogs`
 */
var getMatchingVehicleCatalogs = function (versionId, year, make, model, bodyStyle) {
  // VehicleCatalogs aggregation pipeline query
  // Get just the docs that contain a shapes element where color is 'red'
  return db.getCollection(vehiclecatalogs).aggregate([
    {
      "$match": {
        'versionId': versionId,
        'year': year
      }
    },
    {
      "$unwind": "$makes"
    },
    {
      "$match": {
        "makes.key": String(make)
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
        "models.models.key": String(model)
      }
    },
    {
      "$match": {
        "bodyStyles.bodyStyles.key": String(bodyStyle)
      }
    },
    {
      "$project": {
        "makes._id": 1,
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
                }
              ]
            }
          }
        },
        "_id": 0
      }
    }
  ]);
};

var initiateOperation = function () {
  var versionId = getVehicleMasterForWI()["_id"];
  var quotesCursor = getLegacyQuotesForUpdation();
};