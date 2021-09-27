using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TTQ.Load
{
    public class Writer
    {
        List<object[]> rows = new();
        readonly string table;
        public Writer(string table)
        {
            this.table = table;
            Task.Factory.StartNew(async () => {
                //await using var c = new ClickHouseConnection("Host=click.tt.svc.cluster.local");
                //await c.OpenAsync();

                //var cmd = c.CreateCommand();
                //cmd.CommandText = "create table load (ts DateTime, put Int32, get Int32) ENGINE = MergeTree() order by(ts);";
                //await cmd.ExecuteNonQueryAsync();

                //cmd.CommandText = "create table message (ts DateTime, operation LowCardinality(String), duration Float64, n Int32) ENGINE = SummingMergeTree() order by(ts, operation);";
                //await cmd.ExecuteNonQueryAsync();

                //await c.CloseAsync();

                while (true)
                {
                    try { await Worker(); }
                    catch (Exception e)
                    {
                        //Console.WriteLine(e.ToString());
                        Console.WriteLine(e.Message);
                    }
                    await Task.Delay(1000);
                }
            }, TaskCreationOptions.LongRunning);
        }

        public async Task<ConcurrencyDescriptor> QueryMaxConcurrency()
        {
            try
            {
                await using var c = new ClickHouseConnection("Host=click.tt.svc.cluster.local");
                await c.OpenAsync();

                var cmd = c.CreateCommand();
                cmd.CommandText = "select put, get from load order by ts desc limit 1";
                await using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                    return new ConcurrencyDescriptor { Put = (int)reader[0], Get = (int)reader[1] };
                return new ConcurrencyDescriptor { Put = -1, Get = -1 };
            }
            catch (Exception e)
            {
                //Console.WriteLine(e.ToString());
                Console.WriteLine(e.Message);
                return new ConcurrencyDescriptor { Put = -1, Get = -1 };
            }
        }

        async Task Worker()
        {
            while (true)
            {
                await using var c = new ClickHouseConnection("Host=click.tt.svc.cluster.local");
                await c.OpenAsync();

                using var bulkCopyInterface = new ClickHouseBulkCopy(c)
                {
                    DestinationTableName = table,
                    BatchSize = 100000
                };


                while (true)
                {
                    await Task.Delay(333);
                    if (rows.Count == 0) continue;

                    List<object[]> batch;
                    lock (this)
                    {
                        if (rows.Count == 0) continue;
                        batch = rows;
                        rows = new List<object[]>();
                    }

                    await bulkCopyInterface.WriteToServerAsync(batch);
                }
            }
        }
        public void Write(params object[] row)
        {
            lock (this) rows.Add(row);
        }
    }

    public struct ConcurrencyDescriptor
    {
        public int Put { get; set; }
        public int Get { get; set; }
    }
}
