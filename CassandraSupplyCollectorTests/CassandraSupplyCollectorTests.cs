using System;
using System.Collections.Generic;
using S2.BlackSwan.SupplyCollector.Models;
using System.Linq;
using Xunit;

namespace CassandraSupplyCollectorTests
{
    public class CassandraSupplyCollectorTests
    {

        public readonly CassandraSupplyCollector.CassandraSupplyCollector _instance;
        public readonly DataContainer _container;

        public CassandraSupplyCollectorTests()
        {
            _instance = new CassandraSupplyCollector.CassandraSupplyCollector();

            _container = new DataContainer()
            {
                ConnectionString = _instance.BuildConnectionString("127.0.0.1", "9042", "test", "", "")
            };
        }

        [Fact]
        public void CollectSampleTest()
        {
            var entity = new DataEntity("name", DataType.String, "character varying", _container, new DataCollection(_container, "test_index"));
            var samples = _instance.CollectSample(entity, 5);
            Assert.Equal(5, samples.Count);
            Assert.Contains("Wednesday", samples);
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
            var metrics = new DataCollectionMetrics[] {
                new DataCollectionMetrics()
                    {Name = "test_data_types", RowCount = 1, TotalSpaceKB = 8},
                new DataCollectionMetrics()
                    {Name = "test_index", RowCount = 7, TotalSpaceKB = 8}
            };

            var result = _instance.GetDataCollectionMetrics(_container);
            Assert.Equal(4, result.Count);

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
            Assert.Equal(4, tables.Count);
            Assert.Equal(17, elements.Count);

            foreach (DataEntity element in elements)
            {
                Assert.NotEqual(string.Empty, element.DbDataType);
            }
        }

        [Fact]
        public void GetTableNamesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);
            Assert.Equal(4, tables.Count);

            var tableNames = new string[] { "test_data_types", "test_index", "course", "teacher" };
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
