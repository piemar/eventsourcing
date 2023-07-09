using System;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization;

namespace ChangeStreamExample
{
    class Program
    {
        private static MongoDBConnection? mongoClient;
        public static void CreateSnapshot(String AggregateId)
        {   
            var collection = mongoClient.Database.GetCollection<BsonDocument>(mongoClient.CollectionName);
            var pipeline = new List<BsonDocument>
            {
                new BsonDocument("$match", new BsonDocument
                {
                    { "AggregateId", AggregateId }
                }),
                new BsonDocument("$sort", new BsonDocument
                {
                    { "Timestamp", 1 }
                }),
                new BsonDocument("$group", new BsonDocument
                {
                    { "_id", "$AggregateId" },
                    { "SoftwarePackage", new BsonDocument("$push", new BsonDocument("$cond", new BsonArray
                        {
                            new BsonDocument("$in", new BsonArray
                            {
                                "$Event",
                                new BsonArray
                                {
                                    "SoftwarePackageCreated",
                                    "SoftwarepackageUpdated"
                                }
                            }),
                            new BsonDocument
                            {
                                { "_id", "$$ROOT._id" },
                                { "Id", "$Payload.Id" },
                                { "SoftwarePackageTypeId", "$Payload.SoftwarePackageTypeId" },
                                { "SoftwarePackageType", "$Payload.SoftwarePackageTypeId" },
                                { "ProductNumber", "$Payload.ProductNumber" },
                                { "Name", "$Payload.Name" },
                                { "Description", "$Payload.Description" },
                                { "AccessRightGroupId", "$Payload.AccessRightGroupId" },
                                { "AggregateId", "$$ROOT.AggregateId" },
                                { "Timestamp", "$$ROOT.Timestamp" }
                            },
                            BsonNull.Value
                        })) },
                    { "Version", new BsonDocument("$push", new BsonDocument("$cond", new BsonArray
                        {
                            new BsonDocument("$in", new BsonArray
                            {
                                "$Event",
                                new BsonArray
                                {
                                    "VersionCreated",
                                    "VersionUpdated",
                                    "ProductRStateCreated",
                                    "AcaGroupCreated",
                                    "VersionStateChanged",
                                    "RegisteredInPrim"
                                }
                            }),
                            new BsonDocument
                            {
                                { "_id", "$$ROOT._id" },
                                { "Id", "$Payload.Id" },
                                { "RState", "$Payload.RState" },
                                { "SoftwarePackageId", "$Payload.SoftwarePackageId" },
                                { "State", "$Payload.State" },
                                { "VersionId", "$$ROOT.AggregateId" },
                                { "Timestamp", "$$ROOT.Timestamp" }
                            },
                            BsonNull.Value
                        })) },
                    { "VersionFiles", new BsonDocument("$push", new BsonDocument("$cond", new BsonArray
                        {
                            new BsonDocument("$in", new BsonArray
                            {
                                "$Event",
                                new BsonArray
                                {
                                    "FileAttached",
                                    "FileMetadataUpdated",
                                    "FileMovedToAca"
                                }
                            }),
                            new BsonDocument
                            {
                                { "Id", "$Payload.Id" },
                                { "SoftwarePackageVersionFileTypeId", "$Payload.SoftwarePackageVersionFileTypeId" },
                                { "FileName", "$Payload.FileName" },
                                { "Description", "$Payload.Description" },
                                { "RelativePath", "$Payload.RelativePath" },
                                { "State", "$Payload.State" },
                                { "VersionId", "$$ROOT.AggregateId" },
                                { "Timestamp", "$$ROOT.Timestamp" },
                                { "AcaResourceId", "$Payload.AcaResourceId" }
                            },
                            BsonNull.Value
                        })) },
                    { "Tags", new BsonDocument("$push", new BsonDocument("$cond", new BsonArray
                        {
                            new BsonDocument("$in", new BsonArray
                            {
                                "$Event",
                                new BsonArray
                                {
                                    "TagAdded",
                                    "TagRemoved"
                                }
                            }),
                            new BsonDocument
                            {
                                { "_id", "$$ROOT._id" },
                                { "Id", "$Payload.Id" },
                                { "Name", "$Payload.Name" },
                                { "VersionId", "$$ROOT.VersionId" },
                                { "Timestamp", "$$ROOT.Timestamp" }
                            },
                            BsonNull.Value
                        })) },
                    { "SoftwarePackageVersions", new BsonDocument("$push", new BsonDocument("$cond", new BsonArray
                        {
                            new BsonDocument("$in", new BsonArray
                            {
                                "$Event",
                                new BsonArray
                                {
                                    "SoftwarePackageCreated",
                                    "VersionCreated",
                                    "VersionUpdated",
                                    "ProductRStateCreated",
                                    "VersionStateChanged"
                                }
                            }),
                            new BsonDocument
                            {
                                { "_id", "$$ROOT._id" },
                                { "Id", "$Payload.Id" },
                                { "SoftwarePackageId", "$Payload.SoftwarePackageId" },
                                { "Name", "$Payload.Name" },
                                { "VersionId", "$VersionId" },
                                { "ProductNumber", "$Payload.ProductNumber" },
                                { "SoftwarePackageTypeId", "$Payload.SoftwarePackageTypeId" },
                                { "RState", "$Payload.RState" },
                                { "State", "$Payload.State" }
                            },
                            BsonNull.Value
                        })) },
                    { "DependentVersions", new BsonDocument("$push", new BsonDocument("$cond", new BsonArray
                        {
                            new BsonDocument("$in", new BsonArray
                            {
                                "$Event",
                                new BsonArray
                                {
                                    "DependentVersionAdded",
                                    "DependentVersionRemoved"
                                }
                            }),
                            new BsonDocument
                            {
                                { "_id", "$$ROOT._id" },
                                { "Id", "$Payload.Id" },
                                { "Name", "$Payload.Name" },
                                { "DependentVersionId", "$Payload.DependentVersionId" },
                                { "VersionId", "$$ROOT.VersionId" },
                                { "Timestamp", "$$ROOT.Timestamp" }
                            },
                            BsonNull.Value
                        })) },
                    { "LastUpdatedAt", new BsonDocument("$last", "$Timestamp") },
                }),
                new BsonDocument("$project", new BsonDocument
                {
                    { "_id", 0 },
                    { "AggregateId", "$_id" },
                    { "LastUpdatedAt", 1 },
                    { "SoftwarePackage", new BsonDocument("$filter", new BsonDocument
                        {
                            { "input", "$SoftwarePackage" },
                            { "as", "softwarePackage" },
                            { "cond", new BsonDocument("$ne", new BsonArray
                                {
                                    "$$softwarePackage",
                                    BsonNull.Value
                                }) }
                        }) },
                    { "Version", new BsonDocument("$let", new BsonDocument
                        {
                            { "vars", new BsonDocument("toMerge", new BsonDocument("$filter", new BsonDocument
                                {
                                    { "input", "$Version" },
                                    { "as", "version" },
                                    { "cond", new BsonDocument("$ne", new BsonArray
                                        {
                                            "$$version",
                                            BsonNull.Value
                                        }) }
                                })) },
                            { "in", new BsonDocument("$map", new BsonDocument
                                {
                                    { "input", new BsonDocument("$setIntersection", "$$toMerge.VersionId") },
                                    { "as", "id" },
                                    { "in", new BsonDocument("$reduce", new BsonDocument
                                        {
                                            { "input", new BsonDocument("$filter", new BsonDocument
                                                {
                                                    { "input", "$$toMerge" },
                                                    { "as", "v" },
                                                    { "cond", new BsonDocument("$eq", new BsonArray
                                                        {
                                                            "$$v.VersionId",
                                                            "$$id"
                                                        }) }
                                                }) },
                                            { "initialValue", new BsonDocument() },
                                            { "in", new BsonDocument("$mergeObjects", new BsonArray
                                                {
                                                    "$$value",
                                                    "$$this"
                                                }) }
                                        }) }
                                }) }
                        }) },
                    { "VersionFiles", new BsonDocument("$let", new BsonDocument
                        {
                            { "vars", new BsonDocument("toMerge", new BsonDocument("$filter", new BsonDocument
                                {
                                    { "input", "$VersionFiles" },
                                    { "as", "versionFiles" },
                                    { "cond", new BsonDocument("$ne", new BsonArray
                                        {
                                            "$$versionFiles",
                                            BsonNull.Value
                                        }) }
                                })) },
                            { "in", new BsonDocument("$map", new BsonDocument
                                {
                                    { "input", new BsonDocument("$setIntersection", "$$toMerge.Id") },
                                    { "as", "id" },
                                    { "in", new BsonDocument("$reduce", new BsonDocument
                                        {
                                            { "input", new BsonDocument("$filter", new BsonDocument
                                                {
                                                    { "input", "$$toMerge" },
                                                    { "as", "vf" },
                                                    { "cond", new BsonDocument("$eq", new BsonArray
                                                        {
                                                            "$$vf.Id",
                                                            "$$id"
                                                        }) }
                                                }) },
                                            { "initialValue", new BsonDocument() },
                                            { "in", new BsonDocument("$mergeObjects", new BsonArray
                                                {
                                                    "$$value",
                                                    "$$this"
                                                }) }
                                        }) }
                                }) }
                        }) },
                    { "DependentVersions", new BsonDocument("$filter", new BsonDocument
                        {
                            { "input", "$DependentVersions" },
                            { "as", "dependentVersions" },
                            { "cond", new BsonDocument("$ne", new BsonArray
                                {
                                    "$$dependentVersions",
                                    BsonNull.Value
                                }) }
                        }) },
                    { "Tags", new BsonDocument("$filter", new BsonDocument
                        {
                            { "input", "$Tags" },
                            { "as", "tags" },
                            { "cond", new BsonDocument("$ne", new BsonArray
                                {
                                    "$$tags",
                                    BsonNull.Value
                                }) }
                        }) },
                    { "SoftwarePackageVersions", new BsonDocument("$filter", new BsonDocument
                        {
                            { "input", "$SoftwarePackageVersions" },
                            { "as", "spv" },
                            { "cond", new BsonDocument("$ne", new BsonArray
                                {
                                    "$$spv",
                                    BsonNull.Value
                                }) }
                        }) },
                }),
                new BsonDocument("$lookup", new BsonDocument
                {
                    { "from", "events" },
                    { "let", new BsonDocument("softwarePackageIds", new BsonDocument("$filter", new BsonDocument
                        {
                            { "input", new BsonDocument("$map", new BsonDocument
                                {
                                    { "input", "$SoftwarePackageVersions" },
                                    { "in", "$$this.SoftwarePackageTypeId" }
                                }) },
                            { "cond", new BsonDocument("$ne", new BsonArray
                                {
                                    "$$this",
                                    BsonNull.Value
                                }) }
                        })) },
                    { "pipeline", new BsonArray
                        {
                            new BsonDocument("$match", new BsonDocument("$expr", new BsonDocument("$and", new BsonArray
                                {
                                    new BsonDocument("$eq", new BsonArray
                                        {
                                            "$Event",
                                            "ClassifierCreated"
                                        }),
                                    new BsonDocument("$in", new BsonArray
                                        {
                                            "$AggregateId",
                                            "$$softwarePackageIds"
                                        })
                                })))
                        }},
                    { "as", "Classifiers" }
                }),

                new BsonDocument("$set", new BsonDocument
                {
                    { "Version", new BsonDocument("$map", new BsonDocument
                        {
                            { "input", "$Version" },
                            { "as", "v" },
                            { "in", new BsonDocument("$mergeObjects", new BsonArray
                                {
                                    "$$v",
                                    new BsonDocument("VersionFiles", new BsonDocument("$filter", new BsonDocument
                                        {
                                            { "input", "$VersionFiles" },
                                            { "as", "vf" },
                                            { "cond", new BsonDocument("$eq", new BsonArray
                                                {
                                                    "$$vf.VersionId",
                                                    "$$v.VersionId"
                                                }) }
                                        }))
                                }) }
                        }) },
                    { "SoftwarePackageVersions", new BsonDocument("$map", new BsonDocument
                        {
                            { "input", "$SoftwarePackageVersions" },
                            { "in", new BsonDocument("$mergeObjects", new BsonArray
                                {
                                    "$$this",
                                    new BsonDocument("SoftwarePackageType", new BsonDocument("$getField", new BsonDocument
                                        {
                                            { "input", new BsonDocument("$getField", new BsonDocument
                                                {
                                                    { "field", "Payload" },
                                                    { "input", new BsonDocument("$arrayElemAt", new BsonArray
                                                        {
                                                            new BsonDocument("$filter", new BsonDocument
                                                            {
                                                                { "input", "$Classifiers" },
                                                                { "as", "c" },
                                                                { "cond", new BsonDocument("$eq", new BsonArray
                                                                    {
                                                                        "$$c.AggregateId",
                                                                        "$$this.SoftwarePackageTypeId"
                                                                    }) }
                                                            }),
                                                            0
                                                        }) }
                                                }) },
                                            { "field", "Definition" }
                                        }))
                                }) }
                        }) }
                }),

                new BsonDocument("$unset", "VersionFiles"),

                new BsonDocument("$addFields", new BsonDocument
                {
                    { "Event", "Snapshot" },
                    { "Timestamp", DateTime.Now }
                }),

                new BsonDocument("$merge", new BsonDocument
                {
                    { "into", new BsonDocument("db", "aurora_peter").Add("coll", "events") },
                    { "on", "_id" },
                    { "whenMatched", "replace" },
                    { "whenNotMatched", "insert" }
                })

            };
            var aggregation = collection.Aggregate<BsonDocument>(pipeline);
            
        }
        public static Boolean IsSnapshot(string AggregateId)
        {
            var lastUpdatedSnapshot = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            IMongoCollection<BsonDocument> collection = mongoClient.Database.GetCollection<BsonDocument>(mongoClient.CollectionName);
            var eventsResults = new List<BsonDocument> { };
            var pipelineSnapshotTimeStamp = new List<BsonDocument>
            {
                new BsonDocument("$match", new BsonDocument
                {
                    { "AggregateId", AggregateId },
                    { "Event", "Snapshot" }
                }),
                new BsonDocument("$sort", new BsonDocument
                {
                    { "Timestamp", -1 }
                }),
                new BsonDocument("$limit", 1),
                new BsonDocument("$project", new BsonDocument
                {
                    { "_id", 0 },
                    { "Timestamp", 1 }
                })
            };
            var timeStamp = collection.Aggregate<BsonDocument>(pipelineSnapshotTimeStamp).ToList();
            // Print the Timestamp field to the console
            if (timeStamp.Count > 0)
            {
                lastUpdatedSnapshot = timeStamp.First()["Timestamp"].ToUniversalTime();
            }

            var matchEventsAfterLatestSnapshot = new BsonDocument
                {
                    {
                        "AggregateId", AggregateId
                    },
                    {
                        "Timestamp", new BsonDocument
                        {
                            {
                                "$gte", BsonValue.Create(lastUpdatedSnapshot)
                            }
                        }
                    },
                    {
                        "EventName", new BsonDocument
                        {
                            {
                                "$ne", "Snapshot"
                            }
                        }
                    }
                };
            eventsResults = collection.Find(matchEventsAfterLatestSnapshot).ToList();

            // Check Snapshot creation Threshold
            if (eventsResults.Count == mongoClient.SnapshotThreshold)
            {
                Console.WriteLine($"Creating Snapshot for AggregateId:"+AggregateId);
                return true;
            }
            return false;
        }
        static void Main(string[] args)
        {
            SubscribeToChangeStream();
        }

        private static void SubscribeToChangeStream()
        {
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor = ConfigureChangstream();
            using (var enumerator = cursor.ToEnumerable().GetEnumerator())
            {
                while (true)
                {
                    if (enumerator.MoveNext())
                    {
                        var change = enumerator.Current;
                        var document = change.FullDocument;               
                        string AggregateId = document["AggregateId"].ToString();
                        if (AggregateId != null && IsSnapshot(AggregateId))
                        {
                            CreateSnapshot(AggregateId);

                        }

                    }
                }
            }
        }

        private static IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> ConfigureChangstream()
        {
            mongoClient = new MongoDBConnection();
            var collection = mongoClient.Database.GetCollection<BsonDocument>(mongoClient.CollectionName);
            var options = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
            };
            // Only listen to changes that are inserts
            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
                .Match(change => change.OperationType == ChangeStreamOperationType.Insert);

            var cursor = collection.Watch(pipeline, options);
            return cursor;
        }
    }
}
