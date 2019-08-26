using System;
using System.Collections.Generic;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using System.Linq;
using Cassandra;

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
            Dictionary<string, string> connectionStringValues = GetConnectionStringValues(container.ConnectionString);
            ICluster _cluster = Cluster.Builder().AddContactPoint(connectionStringValues["address"]).Build();
            var tables = _cluster.Metadata.GetKeyspace(connectionStringValues["keyspace"]).GetTablesMetadata();

            var dataEntities = tables.SelectMany(t => GetSchema(connectionStringValues["keyspace"], t.Name, _cluster, container));
            var dataCollections = tables.Select(t => new DataCollection(container, t.Name));

            return (dataCollections.ToList(), dataEntities.ToList());
        }

        private List<DataEntity> GetSchema(string keyspace, string tableName, ICluster cluster, DataContainer container)
        {
            var table = cluster.Metadata.GetTable(keyspace, tableName);
            var dataCollection = new DataCollection(container, tableName);
            var dataEntities = new List<DataEntity>();
            foreach (TableColumn column in table.TableColumns)
            {
                AddEntityAndChildren(cluster, column, container, dataCollection, dataEntities);
            }
            

            return dataEntities;
        }

        private void AddEntityAndChildren(ICluster cluster, TableColumn column, DataContainer container, DataCollection dataCollection, List<DataEntity> entities)
        {
            switch (column.TypeCode)
            {
                case ColumnTypeCode.List:
                    var listInfo = (ListColumnInfo)column.TypeInfo;
                    entities.AddRange(GetDataEntitiesFromArray(cluster, column.Name, listInfo.ValueTypeCode, container, dataCollection));
                    break;
                case ColumnTypeCode.Set:
                    var setInfo = (SetColumnInfo)column.TypeInfo;
                    if (setInfo != null)
                        entities.AddRange(GetDataEntitiesFromArray(cluster, column.Name, setInfo.KeyTypeCode, container, dataCollection));
                    break;
                case ColumnTypeCode.Map:
                    var mapInfo = (MapColumnInfo)column.TypeInfo;
                    entities.AddRange(GetDataEntitiesFromArray(cluster, column.Name, mapInfo.KeyTypeCode, container, dataCollection));
                    entities.AddRange(GetDataEntitiesFromArray(cluster, column.Name, mapInfo.ValueTypeCode, container, dataCollection));
                    break;
                case ColumnTypeCode.Udt:
                    var udtInfo = (UdtColumnInfo)column.TypeInfo;
                    var typeName = udtInfo.Name.Split(".")[1];
                    entities.AddRange(GetDataEntitiesFromUdt(cluster, typeName, container, dataCollection));
                    break;
                default:
                    entities.Add(new DataEntity(column.Name, ConvertDataType(column.TypeCode.ToString()), column.TypeCode.ToString(), container, dataCollection));
                    break;
            }
        }

        private List<DataEntity> GetDataEntitiesFromArray(ICluster cluster, String name, ColumnTypeCode type, DataContainer container, DataCollection dataCollection)
        {
            var entities = new List<DataEntity>();

            TableColumn column = new TableColumn();
            column.Name = name;
            column.TypeCode = type;
            AddEntityAndChildren(cluster, column, container, dataCollection, entities);

            return entities;
        }


        private List<DataEntity> GetDataEntitiesFromUdt(ICluster cluster, String udtName, DataContainer container, DataCollection dataCollection)
        {
            var entities = new List<DataEntity>();
            var udtTable = cluster.Metadata.GetUdtDefinition("test", udtName);
            foreach (var field in udtTable.Fields)
            {
                TableColumn column = new TableColumn();
                column.Name = field.Name;
                column.TypeCode = field.TypeCode;
                AddEntityAndChildren(cluster, column, container, dataCollection, entities);
            }
            return entities;
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

        private DataType ConvertDataType(String dbDataType)
        {
            if ("Int".Equals(dbDataType))
                return DataType.Int;
            else if ("Ascii".Equals(dbDataType))
                return DataType.String;
            else if ("Bigint".Equals(dbDataType))
                return DataType.Long;
            else if ("Boolean".Equals(dbDataType))
                return DataType.Boolean;
            else if ("counter".Equals(dbDataType))
                return DataType.Int;
            else if ("Date".Equals(dbDataType))
                return DataType.DateTime;
            else if ("Double".Equals(dbDataType))
                return DataType.Double;
            else if ("Float".Equals(dbDataType))
                return DataType.Float;
            else if ("Text".Equals(dbDataType))
                return DataType.String;
            else if ("Time".Equals(dbDataType))
                return DataType.DateTime;
            else if ("Timestamp".Equals(dbDataType))
                return DataType.DateTime;
            else if ("Uuid".Equals(dbDataType))
                return DataType.Guid;
            else if ("Varchar".Equals(dbDataType))
                return DataType.String;

            return DataType.Unknown;
        }
    }

    public static class LinqExtensions
    {
        public static IEnumerable<TSource> DistinctBy<TSource, TKey>
            (this IEnumerable<TSource> source, Func<TSource, TKey> keySelector)
        {
            HashSet<TKey> seenKeys = new HashSet<TKey>();
            foreach (TSource element in source)
            {
                if (seenKeys.Add(keySelector(element)))
                {
                    yield return element;
                }
            }
        }
    }

}
