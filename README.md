# CassandraSupplyCollector
A supply collector designed to connect to Cassandra

## Build
Run `dotnet build`

## Tests
Run `./run-tests.sh`

## Known issues
- UDT types are not fully supported. Cassandra doesn't support querying something like `SELECT list.map.udt FROM table`
Requires creating/registering POCO objects to unwrap it when querying from database.
- Random sampling test is disabled - returns unpredictable amount of samples

Suggested algorithm:

Parse database schema and generate code for corresponding POCO objects, compile using CSharpCompilation class. Load wrapper classes and register using UdtMap.For<> method.
Adjust CollectSample() method to split data entity name by dot symbol, and query only first component. 
Wrap the result to corresponding POCO object, and collect samples from it's properties.

- Uses `count(*)` to calculate row count. Must be checked on a few millions records, probably we should find some better alternative.
- Table size is not calculated, there are no methods in C# driver. However, this information is available in `nodetool cfstats` output.
As well as row count. Need to find a way to get this information from C#
- Problem with data types support. Scalar types supported - string,int,double,datetime. Lists - only set<string>, maps - only map<text,int>
