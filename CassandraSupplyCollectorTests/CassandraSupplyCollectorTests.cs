using System.Collections.Generic;
using S2.BlackSwan.SupplyCollector.Models;
using System.Linq;
using Xunit;
using Cassandra;

namespace CassandraSupplyCollectorTests
{
    public class CassandraSupplyCollectorTests
    {
        private readonly CassandraSupplyCollector.CassandraSupplyCollector _instance;
        private readonly DataContainer _container;

        public CassandraSupplyCollectorTests()
        {
            _instance = new CassandraSupplyCollector.CassandraSupplyCollector();

            _container = new DataContainer()
            {
                ConnectionString = _instance.BuildConnectionString("192.168.1.102", 9042, "test", "", "")
            };
        }

        [Fact]
        public void CollectSampleTest()
        {
            var entity = new DataEntity("name", DataType.String, ColumnTypeCode.Text.ToString(), _container, new DataCollection(_container, "test_index"));
            var samples = _instance.CollectSample(entity, 5);
            Assert.Equal(5, samples.Count);
            Assert.Contains("Wednesday", samples);

            entity = new DataEntity("coursenames", DataType.Unknown, ColumnTypeCode.List.ToString(), _container, new DataCollection(_container, "teacher"));
            samples = _instance.CollectSample(entity, 1);
            Assert.Contains("Data Science", samples);

            entity = new DataEntity("prerq", DataType.Unknown, ColumnTypeCode.Map.ToString(), _container, new DataCollection(_container, "course"));
            samples = _instance.CollectSample(entity, 1);
            Assert.Contains("Neural Network", samples);


        }

        [Fact]
        public void DataStoreTypesTest()
        {
            var result = _instance.DataStoreTypes();
            Assert.Contains("Cassandra", result);
        }

        [Fact]
        public void GetDataCollectionMetricsTest()
        {
            var metrics = new [] {
                new DataCollectionMetrics()
                    {Name = "test_data_types", RowCount = 1, TotalSpaceKB = 8},
                new DataCollectionMetrics()
                    {Name = "test_index", RowCount = 7, TotalSpaceKB = 8}
            };

            var result = _instance.GetDataCollectionMetrics(_container);
            Assert.Equal(5, result.Count);

            foreach (var metric in metrics)
            {
                var resultMetric = result.Find(x => x.Name.Equals(metric.Name));
                Assert.NotNull(resultMetric);
                Assert.Equal(metric.RowCount, resultMetric.RowCount);
            }
        }

        [Fact]
        public void GetSchemaTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);
            Assert.Equal(5, tables.Count);
            Assert.Equal(25, elements.Count);

            foreach (DataEntity element in elements)
            {
                Assert.NotEqual(string.Empty, element.DbDataType);
            }

            var columns = elements.Where(x => x.Collection.Name.Equals("teacher")).ToArray();
            Assert.Equal(4, columns.Length);

            columns = elements.Where(x => x.Collection.Name.Equals("course")).ToArray();
            Assert.Equal(3, columns.Length);

            var nestedTypes = new Dictionary<string, string>() {
                {"id", "Int"},
                {"addr.street", "Text"},
                {"addr.zip", "Int"},
                {"addr.phones.alias", "Text"},
                {"addr.phones.number", "Text"},
                {"main_phone.alias", "Text"},
                {"main_phone.number", "Text"}
            };

            columns = elements.Where(x => x.Collection.Name.Equals("user")).ToArray();
            Assert.Equal(nestedTypes.Count, columns.Length);

            foreach (var nestedType in nestedTypes) {
                var col = columns.FirstOrDefault(x => x.Name.Equals(nestedType.Key));
                Assert.True(col != null, $"Column {nestedType.Key} exists");
                Assert.Equal(nestedType.Value, col.DbDataType);
            }
        }

        [Fact]
        public void GetTableNamesTest()
        {
            var (tables, _) = _instance.GetSchema(_container);
            Assert.Equal(5, tables.Count);

            var tableNames = new [] { "test_data_types", "test_index", "course", "teacher", "user" };
            foreach (var tableName in tableNames)
            {
                var table = tables.Find(x => x.Name.Equals(tableName));
                Assert.NotNull(table);
            }
        }

        [Fact]
        public void TestConnectionTest()
        {
            var result = _instance.TestConnection(_container);
            Assert.True(result);
        }

    }
}
