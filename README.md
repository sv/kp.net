# kp.net
Imported from http://kpnet.codeplex.com/ which is shutting down. Original authors are aliaksandr_pedzko, andrei_ivanou, dguidara, ShrikantCGhate

kp.net is the most powerful open ADO.Net provider for KDB+. It supports ADO.Net provider model and connection pooling.
kp.net is designed to bridge the gap between the KDB+ column-oriented internal structures and well-known .Net objects such as IDataReader and DataTable.

I'm a .Net developer currently working on a large project that deals with KDB+ db. 
Though KDB+ provides a simple .Net provider (at https://code.kx.com/trac/browser/kx/kdb%2B/c/c.cs), many other
.Net developers and I are used to various "out of the box" features that ADO.Net provides such as implicit connection pooling, datasets, datareaders etc. There is a great number of nice things in ADO.NET, not to mention LINQ and EF.

These thoughts made me think of implementing an ADO.Net provider for KDB+ to make life easier. I'm aware that it's quiet a decent piece of work,
as such, I gladly invite other enthusiasts to participate in this project.

kp.net provides access to kdb+ on 3 levels:

1. The lowest level is provided by KdbPlusDatabaseClient.

Small example:
```
private static void RunSimplifiedAPIExample()
{
	try
	{
		using (KdbPlusDatabaseClient client = KdbPlusDatabaseClient.Factory.CreateNewClient("server=localhost;port=1001;"))
		{
			// get current time
			TimeSpan time = client.ExecuteScalar<TimeSpan>(".z.T");
			Console.WriteLine("Current time {0}", time);

			// create trade table
			client.ExecuteNonQuery("trade:([]sym:();price:();size:())");
			client.ExecuteNonQuery("`trade insert(`AIG;10.75;200)");
			client.ExecuteOneWayNonQuery("`trade insert(`AIG1;10.75;200)");

			// parameters handling example
			object[] x = { "xx", 93.5, 300 };
			client.ExecuteNonQuery("insert", "trade", x);

			// one-way call example
			object[] y = { "yy", 93.5, 300 };
			client.ExecuteOneWayNonQuery("insert", "trade", y);

			// get data from trade
			IDataReader reader = client.ExecuteQuery("select from trade");
			Console.WriteLine("Trade:");

			Console.WriteLine("{0}\t{1}\t{2}", reader.GetName(0), reader.GetName(1), reader.GetName(2));
			while (reader.Read())
			{
				Console.WriteLine("{0}\t{1}\t{2}", reader.GetString(0), reader.GetDouble(1), reader.GetInt32(2));
			}
		}
	}
	catch (KdbPlusException ex)
	{
		Console.WriteLine(ex);
	}
	catch (Exception ex)
	{
		Console.WriteLine(ex);
	}
}
```
2. The middle-level is provided by the PooledKdbPlusDatabaseClient that can be created by KdbPlusDatabaseClient.Factory.CreatePooledClient class.
It has the same methods as KdbPlusDatabaseClient and uses it under the hood.
The main feature of this class is out of the box connection pooling.

3. And finally kp.net provides an implementation of ADO.Net provider classes for kdb+. These classes also use connection pooling that can be turned off.
Here is a small usage example:
```
private static void RunADONetExample()
{
	try
	{
		// full connection string syntax example: 
		//server=1;port=2;user id=3;password=4;buffersize=16384;pooling=false;min pool size=10;max pool size=11;load balance timeout=10
		using (DbConnection connection = new KdbPlusConnection("server=localhost;port=1001;user id=me;password=my;buffersize=16384"))
		{
			// open connection - connection pooling is enabled
			// to disable pooling - use Pooling=false in connection string.
			connection.Open();

			DbCommand command = connection.CreateCommand();

			// get current time
			command.CommandText = ".z.T";
			TimeSpan time = (TimeSpan)command.ExecuteScalar();
			Console.WriteLine("Current time {0}", time);

			// create trade table
			command.CommandText = "trade:([]sym:();price:();size:())";
			command.ExecuteNonQuery();

			command.CommandText = "`trade insert(`AIG;10.75;200)";
			command.ExecuteNonQuery();

			object[] x = { "xx", 93.5, 300 };
			command.CommandText = "insert";

			DbParameter table = command.CreateParameter();
			table.Value = "trade";
			command.Parameters.Add(table);

			DbParameter row = command.CreateParameter();
			row.Value = x;
			command.Parameters.Add(row);
			command.ExecuteNonQuery();

			command.ExecuteNonQuery();

			command.CommandText = "select from trade where sym=@sym";
			command.Parameters.Clear();
			DbParameter sym = command.CreateParameter();
			sym.ParameterName = "@sym";
			sym.Value = "AIG";
			command.Parameters.Add(sym);

			// get data from trade
			IDataReader reader = command.ExecuteReader();
			Console.WriteLine("Trade:");

			Console.WriteLine("{0}\t{1}\t{2}", reader.GetName(0), reader.GetName(1), reader.GetName(2));
			while (reader.Read())
			{
				Console.WriteLine("{0}\t{1}\t{2}", reader.GetString(0), reader.GetDouble(1), reader.GetInt32(2));
			}
		}
	}
	catch (KdbPlusException ex)
	{
		Console.WriteLine(ex);
	}
	catch (Exception ex)
	{
		Console.WriteLine(ex);
	}
}
```
