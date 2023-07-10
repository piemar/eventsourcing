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
        private string databaseName="aurora";
        private MongoClient client;
        private IMongoDatabase database;
        public MongoDBConnection()
        {
            string connectionString = Environment.GetEnvironmentVariable("MONGODB_URL");
            string databaseName = Environment.GetEnvironmentVariable("DATABASE_NAME");
            string collectionName = Environment.GetEnvironmentVariable("COLLECTION_NAME");
            int snapshotThreshold;
            int.TryParse(Environment.GetEnvironmentVariable("SNAPSHOT_THRESHOLD"), out snapshotThreshold) ;

            if (connectionString != null && databaseName != null)
            {
                this.client = new MongoClient(connectionString);
                // Get the database
                this.collectionName = collectionName;
                this.database = this.client.GetDatabase(databaseName);
                this.databaseName = databaseName;
                this.snapshotThreshold=snapshotThreshold;
            }
            else
            {
                Console.WriteLine("Please set Environment variables: either MONGODB_URL or DATABASE_NAME");
                Environment.Exit(0);
            }

        }
        public MongoClient MongoClient => this.client;
        public string CollectionName => this.collectionName;
        public string DatabaseName => this.databaseName;
        public int SnapshotThreshold => this.snapshotThreshold;
        public IMongoDatabase Database => this.database;

    }
}
