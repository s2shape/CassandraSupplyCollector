using System;
using System.Collections.Generic;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using Cassandra;
using System.Linq;

namespace CassandraSupplyCollector
{
    public class CassandraSupplyCollector : SupplyCollectorBase
    {
        public CassandraSupplyCollector()
        {

        }

        public String BuildConnectionString(String address, String port, String keyspace, String username, String password)
        {
            var connectionString = "";

            connectionString = address + "/";
            connectionString += port + "/";
            connectionString += keyspace + "/";
            connectionString += username + "/";
            connectionString += password;

            return connectionString;
        }


        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize)
        {
            var result = new List<string>();

            Dictionary<string, string> connectionStringValues = GetConnectionStringValues(dataEntity.Container.ConnectionString);
            try
            {
                ICluster _cluster = Cluster.Builder().AddContactPoint(connectionStringValues["address"]).Build();
                ISession _session = _cluster.Connect();
                _session.Execute("USE " + connectionStringValues["keyspace"]);

                string query = $"SELECT {dataEntity.Name} FROM {dataEntity.Collection.Name} LIMIT {sampleSize}";
                RowSet res = _session.Execute(query);

                var rows = res.GetRows().ToList();
                if (rows.Count() > 0)
                {
                    foreach(Row row in rows)
                    {
                        result.Add(row.GetValue<string>(dataEntity.Name));
                    }
                }
                return result;
            }
            catch (Exception)
            {
                return result;
            }

        }

        public override List<string> DataStoreTypes()
        {
            return (new[] { "Cassandra" }).ToList();
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container)
        {
            var dataCollectionMetrics = new List<DataCollectionMetrics>();

            Dictionary<string, string> connectionStringValues = GetConnectionStringValues(container.ConnectionString);
            try
            {
                ICluster _cluster = Cluster.Builder().AddContactPoint(connectionStringValues["address"]).Build();
                ISession _session = _cluster.Connect();

                string query = "select * from system_schema.tables where keyspace_name = '" + connectionStringValues["keyspace"]  + "'";
                RowSet res = _session.Execute(query);

                var rows = res.GetRows().ToList();
                if (rows.Count() > 0)
                {
                    foreach (Row row in rows)
                    {
                        var table_name = row.GetValue<string>("table_name");
                        string queryCnt = "select * from " + connectionStringValues["keyspace"] + "." + table_name;
                        RowSet resCnt = _session.Execute(queryCnt);
                        var rowCnt = resCnt.GetRows().ToList();

                        var metrics = new DataCollectionMetrics();
                        metrics.Name = table_name;
                        metrics.RowCount = rowCnt.Count();

                        dataCollectionMetrics.Add(metrics);

                    }
                }

            }
            catch (Exception)
            {
                return dataCollectionMetrics;
            }

            return dataCollectionMetrics;
        }

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container)
        {
            var collections = new List<DataCollection>();
            var entities = new List<DataEntity>();

            Dictionary<string, string> connectionStringValues = GetConnectionStringValues(container.ConnectionString);
            try
            {
                ICluster _cluster = Cluster.Builder().AddContactPoint(connectionStringValues["address"]).Build();
                ISession _session = _cluster.Connect();
                _session.Execute("USE " + connectionStringValues["keyspace"]);

                string query = "select * from system_schema.columns where keyspace_name = '" + connectionStringValues["keyspace"] + "'";
                RowSet res = _session.Execute(query);

                var rows = res.GetRows().ToList();
                if (rows.Count() > 0)
                {
                    DataCollection collection = null;
                    foreach (Row row in rows)
                    {
                        var table = row.GetValue<string>("table_name");
                        var columnName = row.GetValue<string>("column_name");
                        var dataType = row.GetValue<string>("type");

                        if (collection == null || !collection.Name.Equals(table))
                        {
                            collection = new DataCollection(container, table);
                            collections.Add(collection);
                        }

                        entities.Add(new DataEntity(columnName, ConvertDataType(dataType), dataType, container, collection) {

                        });
                    }
                }
            }
            catch (Exception)
            {
                return (collections, entities);
            }


            return (collections, entities);
        }

        public override bool TestConnection(DataContainer container)
        {
            Dictionary<string, string> connectionStringValues = GetConnectionStringValues(container.ConnectionString);

            try {
                ICluster _cluster = Cluster.Builder().AddContactPoint(connectionStringValues["address"]).Build();
                _cluster.Connect();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private Dictionary<string, string>  GetConnectionStringValues(String connectionString)
        {
            string[] values = connectionString.Split("/");
            var _connectionStringValues = new Dictionary<string, string>();
            _connectionStringValues["address"] = values[0];
            _connectionStringValues["port"] = values[1];
            _connectionStringValues["keyspace"] = values[2];
            _connectionStringValues["username"] = values[3];
            _connectionStringValues["password"] = values[4];

            return _connectionStringValues;
        }

        private DataType ConvertDataType(string dbDataType)
        {
            if ("int".Equals(dbDataType))
                return DataType.Int;
            else if ("ascii".Equals(dbDataType))
                return DataType.String;
            else if ("bigint".Equals(dbDataType))
                return DataType.Long;
            else if ("boolean".Equals(dbDataType))
                return DataType.Boolean;
            else if ("counter".Equals(dbDataType))
                return DataType.Int;
            else if ("date".Equals(dbDataType))
                return DataType.DateTime;
            else if ("double".Equals(dbDataType))
                return DataType.Double;
            else if ("float".Equals(dbDataType))
                return DataType.Float;
            else if ("text".Equals(dbDataType))
                return DataType.String;
            else if ("time".Equals(dbDataType))
                return DataType.DateTime;
            else if ("timestamp".Equals(dbDataType))
                return DataType.DateTime;
            else if ("uuid".Equals(dbDataType))
                return DataType.Guid;
            else if ("varchar".Equals(dbDataType))
                return DataType.String;

            return DataType.Unknown;
        }
    }
}
