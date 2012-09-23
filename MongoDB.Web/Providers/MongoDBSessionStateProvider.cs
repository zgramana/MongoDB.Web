using System;
using System.Collections.Specialized;
using System.Configuration;
using System.IO;
using System.Linq.Expressions;
using System.Web;
using System.Web.Configuration;
using System.Web.Hosting;
using System.Web.SessionState;

using MongoDB.Web.Config;
using MongoDB.Web.Common;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

using CommonCore.Cache;

namespace MongoDB.Web.Providers
{
	public interface ISessionStateData
	{
		string ApplicationVirtualPath { get; set; }
		string SessionId { get; set; }

		DateTime Created { get; set; }
		DateTime Expires { get; set; }
		DateTime LockDate { get; set; }
		bool Locked { get; set; }
		int LockId { get; set; }
		SessionStateActions SessionStateActions { get; set; }
		byte[] SessionStateItems { get; set; }
		int SessionStateItemsCount { get; set; }
		int Timeout { get; set; }
	}

	public class DefaultSessionStateData : ISessionStateData
	{
		public class UniqueIdentifier
		{
			public string ApplicationVirtualPath { get; set; }
			public string SessionId { get; set; }
		}

		public DefaultSessionStateData()
		{
			Id = new UniqueIdentifier();
		}

		[BsonId]
		public UniqueIdentifier Id { get; set; }

		[BsonElement("applicationVirtualPath")]
		public string ApplicationVirtualPath
		{
			get { return Id.ApplicationVirtualPath; }
			set { Id.ApplicationVirtualPath = value; }
		}

		[BsonElement("id")]
		public string SessionId
		{
			get { return Id.SessionId; }
			set { Id.SessionId = value; }
		}

		[BsonElement("created")]
		public DateTime Created { get; set; }

		[BsonElement("expires")]
		public DateTime Expires { get; set; }

		[BsonElement("lockDate")]
		public DateTime LockDate { get; set; }

		[BsonElement("locked")]
		public bool Locked { get; set; }

		[BsonElement("lockId")]
		public int LockId { get; set; }

		[BsonElement("sessionStateActions")]
		public SessionStateActions SessionStateActions { get; set; }

		[BsonElement("sessionStateItems")]
		public byte[] SessionStateItems { get; set; }

		[BsonElement("sessionStateItemsCount")]
		public int SessionStateItemsCount { get; set; }

		[BsonElement("timeout")]
		public int Timeout { get; set; }
	}

	public class MongoSessionStateProvider<T> : SessionStateStoreProviderBase
		where T : ISessionStateData, new()
    {
		private MongoCollection<T> _MongoCollection;
        private SessionStateSection _SessionStateSection;
		private MongoDbWebSection _MongoWebSection;
		private BsonClassMap<T> _SessionDataClassMap;
		private static Cache<string, T> _Cache;
		private static object _CacheGuarantee = new object();
		private static MemberHelper<T> _MemberHelper = new MemberHelper<T>();

		private string _IdField;
		private string _ApplicationVirtualPathField;
		private string _LockIdField;
		private string _LockedField;
		private string _ExpiresField;
		private string _SessionStateActionsField;
		private string _LockDateField;

        public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
        {
            return new SessionStateStoreData(new SessionStateItemCollection(), SessionStateUtility.GetSessionStaticObjects(context), timeout);
        }

        public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
        {
            throw new NotImplementedException();
        }

        public override void Dispose()
        {
        }

        public override void EndRequest(HttpContext context)
        {
        }

        public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
        {
            return GetSessionStateStoreData(false, context, id, out locked, out lockAge, out lockId, out actions);
        }

        public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
        {
            return GetSessionStateStoreData(true, context, id, out locked, out lockAge, out lockId, out actions);
        }

        public override void Initialize(string name, NameValueCollection config)
        {
            this._SessionStateSection = ConfigurationManager.GetSection("system.web/sessionState") as SessionStateSection;
			this._MongoWebSection = ConfigurationManager.GetSection("mongoDbWeb") as MongoDbWebSection;

			this._MongoCollection = MongoServer.Create(ConnectionHelper.GetDatabaseConnectionString(_MongoWebSection, config))
				.GetDatabase(ConnectionHelper.GetDatabaseName(_MongoWebSection, config))
				.GetCollection<T>(config["collection"] ?? _MongoWebSection.SessionState.MongoCollectionName);

			_SessionDataClassMap = new BsonClassMap<T>();
			_SessionDataClassMap.AutoMap();

			_ApplicationVirtualPathField = MapBsonMember(t => t.ApplicationVirtualPath);
			_IdField = MapBsonMember(t => t.SessionId);
			_LockIdField = MapBsonMember(t => t.LockId);
			_LockedField = MapBsonMember(t => t.Locked);
			_ExpiresField = MapBsonMember(t => t.Expires);
			_LockDateField = MapBsonMember(t => t.LockDate);
			_SessionStateActionsField = MapBsonMember(t => t.SessionStateActions);

			// Ideally, we'd just use the object's bson id - however, serialization gets a bit wonky
			// when trying to have an Id property which isn't a simple type in the base class
			// This also provides better backwards compatibility with the old BsonDocument implementation (field names match)
			this._MongoCollection.EnsureIndex(
				IndexKeys.Ascending(_ApplicationVirtualPathField, _IdField), IndexOptions.SetUnique(true));

			if (_Cache == null)
			{
				lock (_CacheGuarantee)
				{
					if (_Cache == null)
					{
						_Cache = new Cache<string, T>.Builder()
						{
							EntryExpiration = new TimeSpan(0, 0, _MongoWebSection.SessionState.MemoryCacheExpireSeconds),
							MaxEntries = _MongoWebSection.SessionState.MaxInMemoryCachedSessions
						}.Cache;
					}
				}
			}

            base.Initialize(name, config);
        }

        public override void InitializeRequest(HttpContext context)
        {
        }

        public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
        {
			var query = LookupQuery(id, lockId);

			var newExpires = DateTime.UtcNow.Add(_SessionStateSection.Timeout);
			T session;
			if (_Cache.TryGetValue(id, out session))
			{
				lock (session)
				{
					session.Expires = newExpires;
					session.Locked = false;
				}
			}
			var update = Update.Set(_ExpiresField, newExpires).Set(_LockedField, false);
            this._MongoCollection.Update(query, update);
        }

        public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
        {
			this._MongoCollection.Remove(LookupQuery(id, lockId));
			_Cache.Remove(id);
        }

        public override void ResetItemTimeout(HttpContext context, string id)
        {
			var query = LookupQuery(id);

			var newExpires = DateTime.UtcNow.Add(_SessionStateSection.Timeout);
			T session;
			if (_Cache.TryGetValue(id, out session))
			{
				lock (session)
				{
					session.Expires = newExpires;
				}
			}
			var update = Update.Set(_ExpiresField, newExpires);
            this._MongoCollection.Update(query, update);
        }

        public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem)
        {
            using (var memoryStream = new MemoryStream())
            {
                using (var binaryWriter = new BinaryWriter(memoryStream))
                {
                    ((SessionStateItemCollection)item.Items).Serialize(binaryWriter);

					var sessionData = new T()
					{
						SessionId = id,
						ApplicationVirtualPath = HostingEnvironment.ApplicationVirtualPath,
						Created = DateTime.UtcNow,
						Expires = DateTime.UtcNow.AddMinutes(item.Timeout),
						LockDate = DateTime.UtcNow,
						Locked = false,
						LockId = 0,
						SessionStateActions = SessionStateActions.None,
						SessionStateItems = memoryStream.ToArray(),
						SessionStateItemsCount = item.Items.Count,
						Timeout = item.Timeout
					};

					this._MongoCollection.Save(sessionData, new SafeMode(true, true));
					_Cache[id] = sessionData;
                }
            }
        }

        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
        {
            return false;
        }

        #region Private Methods

        private SessionStateStoreData GetSessionStateStoreData(bool exclusive, HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
        {
            actions = SessionStateActions.None;
            lockAge = TimeSpan.Zero;
            locked = false;
            lockId = null;

			var lookupQuery = LookupQuery(id);
			T session;
			if (!_Cache.TryGetValue(id, out session))
			{
				session = this._MongoCollection.FindOneAs<T>(lookupQuery);
				if (session != null)
					_Cache[id] = session;
			}

            if (session == null)
            {
                locked = false;
            }
            else if (session.Expires <= DateTime.UtcNow)
            {
                locked = false;
				this._MongoCollection.Remove(lookupQuery);
				_Cache.Remove(session.SessionId);
            }
            else if (session.Locked)
            {
                lockAge = DateTime.UtcNow.Subtract(session.LockDate);
                locked = true;
                lockId = session.LockId;
            }
            else
            {
                locked = false;
                lockId = session.LockId;
				actions = session.SessionStateActions;
            }

            if (exclusive && (session != null))
            {
                actions = SessionStateActions.None;

				lock (session)
				{
					session.LockDate = DateTime.UtcNow;
					session.LockId++;
					session.Locked = true;
					session.SessionStateActions = SessionStateActions.None;
				}

				var update = Update.Set(_LockDateField, session.LockDate)
					.Set(_LockIdField, session.LockId)
					.Set(_LockedField, session.Locked)
					.Set(_SessionStateActionsField, session.SessionStateActions);
                this._MongoCollection.Update(lookupQuery, update);
            }

            if ((actions == SessionStateActions.InitializeItem) || (session == null))
            {
                return CreateNewStoreData(context, _SessionStateSection.Timeout.Minutes);
            }

            using (var memoryStream = new MemoryStream(session.SessionStateItems))
            {
                var sessionStateItems = new SessionStateItemCollection();

                if (memoryStream.Length > 0)
                    sessionStateItems = SessionStateItemCollection.Deserialize(new BinaryReader(memoryStream));

                return new SessionStateStoreData(sessionStateItems, SessionStateUtility.GetSessionStaticObjects(context), session.Timeout);
            }
        }

		private IMongoQuery LookupQuery (string id)
		{
			return Query.And(
				Query.EQ(_ApplicationVirtualPathField, HostingEnvironment.ApplicationVirtualPath), 
				Query.EQ(_IdField, id));
		}

		private IMongoQuery LookupQuery(string id, object lockId)
		{
			return Query.And(
				Query.EQ(_ApplicationVirtualPathField, HostingEnvironment.ApplicationVirtualPath),
				Query.EQ(_IdField, id),
				Query.EQ(_LockIdField, lockId.ToString()));
		}

		private string MapBsonMember<TReturn>(Expression<Func<T, TReturn>> expression)
		{
			return _SessionDataClassMap.MapMember(_MemberHelper.GetMember(expression)).ElementName;
		}
        #endregion
    }

	public class MongoDBSessionStateProvider : MongoSessionStateProvider<DefaultSessionStateData>
	{
	}
}