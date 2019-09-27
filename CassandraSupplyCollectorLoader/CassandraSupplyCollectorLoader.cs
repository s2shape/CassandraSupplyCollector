using System;
using System.IO;
using System.Text;
using Cassandra;
using CassandraSupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using SupplyCollectorDataLoader;

namespace CassandraSupplyCollectorLoader
{
    public class CassandraSupplyCollectorLoader : SupplyCollectorDataLoaderBase
    {
        public override void InitializeDatabase(DataContainer dataContainer) {
            var connection = CassandraConnectionString.Parse(dataContainer.ConnectionString);

            using (var cluster = Cluster.Builder().AddContactPoint(connection.Address).Build()) {
                using (var session = cluster.Connect()) {
                    session.Execute($"CREATE KEYSPACE IF NOT EXISTS {connection.Keyspace} WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }};");
                }
            }
        }

        public override void LoadSamples(DataEntity[] dataEntities, long count) {
            var connection = CassandraConnectionString.Parse(dataEntities[0].Container.ConnectionString);

            using (var cluster = Cluster.Builder().AddContactPoint(connection.Address).Build())
            {
                using (var session = cluster.Connect())
                {
                    session.Execute($"USE {connection.Keyspace};");


                    var sb = new StringBuilder();
                    sb.Append("CREATE TABLE ");
                    sb.Append(dataEntities[0].Collection.Name);
                    sb.Append(" (\n");
                    sb.Append("id_field int PRIMARY KEY");

                    foreach (var dataEntity in dataEntities)
                    {
                        sb.Append(",\n");
                        sb.Append(dataEntity.Name);
                        sb.Append(" ");

                        switch (dataEntity.DataType)
                        {
                            case DataType.String:
                                sb.Append("text");
                                break;
                            case DataType.Int:
                                sb.Append("int");
                                break;
                            case DataType.Double:
                                sb.Append("double");
                                break;
                            case DataType.Boolean:
                                sb.Append("boolean");
                                break;
                            case DataType.DateTime:
                                sb.Append("timestamp");
                                break;
                            default:
                                sb.Append("int");
                                break;
                        }

                        sb.AppendLine();
                    }

                    sb.Append(");");

                    session.Execute(sb.ToString());

                    var r = new Random();
                    long rows = 0;
                    while (rows < count)
                    {
                        long bulkSize = 100;
                        if (bulkSize + rows > count)
                            bulkSize = count - rows;

                        sb = new StringBuilder();
                        sb.Append("INSERT INTO ");
                        sb.Append(dataEntities[0].Collection.Name);
                        sb.Append("( id_field");

                        
                        foreach (var dataEntity in dataEntities)
                        {
                            sb.Append(", ");
                            sb.Append(dataEntity.Name);
                        }
                        sb.Append(") VALUES (");

                        var fieldNames = sb.ToString();

                        sb.Clear();
                        sb.Append("BEGIN BATCH\n");

                        for (int i = 0; i < bulkSize; i++) {
                            sb.Append(fieldNames);

                            sb.Append(rows + i);

                            foreach (var dataEntity in dataEntities) {
                                sb.Append(", ");

                                switch (dataEntity.DataType)
                                {
                                    case DataType.String:
                                        sb.Append("'");
                                        sb.Append(Guid.NewGuid().ToString());
                                        sb.Append("'");
                                        break;
                                    case DataType.Int:
                                        sb.Append(r.Next().ToString());
                                        break;
                                    case DataType.Double:
                                        sb.Append(r.NextDouble().ToString().Replace(",", "."));
                                        break;
                                    case DataType.Boolean:
                                        sb.Append(r.Next(100) > 50 ? "true" : "false");
                                        break;
                                    case DataType.DateTime:
                                        var val = DateTimeOffset
                                            .FromUnixTimeMilliseconds(
                                                DateTimeOffset.Now.ToUnixTimeMilliseconds() + r.Next()).DateTime;
                                        sb.Append("'");
                                        sb.Append(val.ToString("s"));
                                        sb.Append("'");
                                        break;
                                    default:
                                        sb.Append(r.Next().ToString());
                                        break;
                                }
                            }

                            sb.Append(");\n");
                        }

                        sb.Append("APPLY BATCH\n");

                        session.Execute(sb.ToString());

                        rows += bulkSize;
                        Console.Write(".");
                    }

                    Console.WriteLine();

                }
            }
        }

        public override void LoadUnitTestData(DataContainer dataContainer) {
            var connection = CassandraConnectionString.Parse(dataContainer.ConnectionString);

            using (var cluster = Cluster.Builder().AddContactPoint(connection.Address).Build())
            {
                using (var session = cluster.Connect())
                {
                    session.Execute($"USE {connection.Keyspace};");

                    using (var reader = new StreamReader("tests/data.sql"))
                    {
                        var sb = new StringBuilder();
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            if (String.IsNullOrEmpty(line))
                                continue;

                            sb.AppendLine(line);
                            if (line.TrimEnd().EndsWith(";")) {
                                session.Execute(sb.ToString());
                                sb.Clear();
                            }
                        }
                    }
                }
            }
        }
    }
}
