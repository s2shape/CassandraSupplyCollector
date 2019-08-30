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
        public string BuildConnectionString(String address, int port, String keyspace, String username, String password) {
            return new CassandraConnectionString(address, port, keyspace, username, password).Build();
        }

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize)
        {
            var result = new List<string>();

            var connection = CassandraConnectionString.Parse(dataEntity.Container.ConnectionString);
            try
            {
                using (var cluster = Cluster.Builder().AddContactPoint(connection.Address).Build()) {
                    using (var session = cluster.Connect()) {
                        session.Execute($"USE {connection.Keyspace}");

                        string query = $"SELECT COUNT(*) FROM {dataEntity.Collection.Name}";
                        RowSet res = session.Execute(query);
                        var rowCount = res.GetRows().First();
                        long totalRows = (long) rowCount[0];

                        double pct = 0.1 + (double)sampleSize / (totalRows <= 0 ? sampleSize : totalRows);
                        var r = new Random();

                        query = $"SELECT {dataEntity.Name} FROM {dataEntity.Collection.Name} LIMIT {sampleSize}";
                        res = session.Execute(query);
                        var rows = res.GetRows();
                        foreach (Row row in rows) {
                            if (dataEntity.DbDataType.Equals(ColumnTypeCode.Text.ToString())) {
                                if (r.NextDouble() < pct)
                                    result.Add(row.GetValue<string>(dataEntity.Name));
                            }
                            else if (dataEntity.DbDataType.Equals(ColumnTypeCode.List.ToString())) {
                                var list = row.GetValue<List<string>>(dataEntity.Name);
                                foreach (var item in list)
                                    if (r.NextDouble() < pct)
                                        result.Add(item);
                            }
                            else if (dataEntity.DbDataType.Equals(ColumnTypeCode.Map.ToString())) {
                                var list = row.GetValue<IDictionary<string, int>>(dataEntity.Name);
                                foreach (var item in list)
                                    if (r.NextDouble() < pct)
                                        result.Add(item.Key);
                            }
                        }
                    }
                }
            }
            catch (Exception)
            {
                // Nothing. TODO: rethrow or log
            }

            return result;
        }

        public override List<string> DataStoreTypes()
        {
            return (new[] { "Cassandra" }).ToList();
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container)
        {
            var dataCollectionMetrics = new List<DataCollectionMetrics>();

            var connection = CassandraConnectionString.Parse(container.ConnectionString);
            try
            {
                using (var cluster = Cluster.Builder().AddContactPoint(connection.Address).Build()) {
                    using (var session = cluster.Connect()) {

                        string query = $"select * from system_schema.tables where keyspace_name = '{connection.Keyspace}'";
                        RowSet res = session.Execute(query);

                        var rows = res.GetRows();
                        foreach (Row row in rows) {
                            var tableName = row.GetValue<string>("table_name");
                            string queryCnt = $"select count(*) from {connection.Keyspace}.{tableName}";
                            RowSet resCnt = session.Execute(queryCnt);
                            var rowCnt = resCnt.First();

                            dataCollectionMetrics.Add(new DataCollectionMetrics() {
                                Name = tableName,
                                RowCount = (long) rowCnt[0]
                            }); //TODO: what about space usage?
                        }
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
            var connection = CassandraConnectionString.Parse(container.ConnectionString);

            using (var cluster = Cluster.Builder().AddContactPoint(connection.Address).Build()) {
                var tables = cluster.Metadata.GetKeyspace(connection.Keyspace).GetTablesMetadata().ToList();

                var dataEntities = tables.SelectMany(t =>
                    GetSchema(connection.Keyspace, t.Name, cluster, container)).ToList();
                var dataCollections = tables.Select(t => new DataCollection(container, t.Name)).ToList();

                return (dataCollections, dataEntities);
            }
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
            var connection = CassandraConnectionString.Parse(container.ConnectionString);

            try {
                using (var cluster = Cluster.Builder().AddContactPoint(connection.Address).Build()) {
                    cluster.Connect();
                }
                return true;
            }
            catch (Exception)
            {
                return false;
            }
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
}
