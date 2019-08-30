using System;
using System.Collections.Generic;
using System.Text;

namespace CassandraSupplyCollector
{
    public class CassandraConnectionString
    {
        public string Address { get; set; }
        public int Port { get; set; }
        public string Keyspace { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        public CassandraConnectionString() {
        }

        public CassandraConnectionString(string address, int port, string keyspace, string username, string password) {
            Address = address;
            Port = port;
            Keyspace = keyspace;
            Username = username;
            Password = password;
        }

        public string Build() {
            return $"{Address}/{Port}/{Keyspace}/{Username}/{Password}";
        }

        public static CassandraConnectionString Parse(string connectionString) {
            string[] values = connectionString.Split("/");
            if (values.Length != 5) {
                throw new ArgumentException("Invalid connection string!");
            }

            return new CassandraConnectionString(values[0], Int32.Parse(values[1]), values[2], values[3], values[4]);
        }
    }
}
