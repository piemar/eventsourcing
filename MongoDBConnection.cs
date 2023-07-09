using System;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization;

namespace ChangeStreamExample
{

    // Create a MongoDB client
    class MongoDBConnection
    {
        private int snapshotThreshold = 100;
        private string collectionName="events";
        private MongoClient client;
        private IMongoDatabase database;
        public MongoDBConnection()
        {
            string connectionString = Environment.GetEnvironmentVariable("MONGODB_URL");
            string databaseName = Environment.GetEnvironmentVariable("DATABASE_NAME");
            string collectionName = Environment.GetEnvironmentVariable("COLLECTION_NAME");
            string snapshotThreshold = Environment.GetEnvironmentVariable("SNAPSHOT_THRESHOLD");

            if (connectionString != null && databaseName != null)
            {
                this.client = new MongoClient(connectionString);
                // Get the database
                this.database = this.client.GetDatabase(databaseName);
            }
            else
            {
                Console.WriteLine("Please set Environment variables: either MONGODB_URL or DATABASE_NAME");
                Environment.Exit(0);
            }

        }
        public MongoClient MongoClient => this.client;
        public string CollectionName => this.collectionName;
        public int SnapshotThreshold => this.snapshotThreshold;
        public IMongoDatabase Database => this.database;

    }
}
