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
            Assert.Equal(2, result.Count);

            foreach (var metric in metrics)
            {
                var resultMetric = result.Find(x => x.Name.Equals(metric.Name));
                Assert.NotNull(resultMetric);
                Assert.Equal(metric.RowCount, resultMetric.RowCount);
            }
        }

        [Fact]
        public void DataTypesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);
            var dataTypes = new Dictionary<string, string>() {
                {"int_field", "int"},
                {"text_field", "text"},
                {"bool_field", "boolean"},
                {"float_field", "float"},
                {"double_field", "double"},
                {"date_field", "date"},
                {"time_field", "time"},
                {"timestamp_field", "timestamp"},
                {"uuid_field", "uuid"}
            };
            var columns = elements.Where(x => x.Collection.Name.Equals("test_data_types")).ToArray();
            Assert.Equal(9, columns.Length);

            foreach (var column in columns)
            {
                Assert.Contains(column.Name, (IDictionary<string, string>)dataTypes);
                Assert.Equal(column.DbDataType, dataTypes[column.Name]);
            }
        }

        [Fact]
        public void GetTableNamesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);
            Assert.Equal(2, tables.Count);
            Assert.Equal(11, elements.Count);

            var tableNames = new string[] { "test_data_types", "test_index"};
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
