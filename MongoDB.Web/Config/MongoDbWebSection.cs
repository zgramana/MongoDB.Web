using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

using CommonCore.Configuration;

namespace MongoDB.Web.Config
{
	class MongoDbWebSection : AutoConfigurationSection
	{
		[ConfigurationProperty("connectionString", DefaultValue = "mongodb://localhost")]
		public string ConnectionString { get; set; }
		[ConfigurationProperty("databaseName", DefaultValue = "ASPNETDB")]
		public string DatabaseName { get; set; }
		[ConfigurationProperty("mongoCollectionName", DefaultValue = "SessionState")]
		public string MongoCollectionName { get; set; }
		[ConfigurationProperty("cacheExpireSeconds", DefaultValue = 100)]
		public int CacheExpireSeconds { get; set; }
		[ConfigurationProperty("maxCachedSessions", DefaultValue = 10000L)]
		public long MaxCachedSessions { get; set; }
	}
}
