using TTQ.Load;
using TTQ.Manager;

var N = Math.Max(1, Convert.ToInt32(Environment.GetEnvironmentVariable("TTQL_N")));
Console.WriteLine($"QUEUES\t{N}");

var payloadSize = 50;
var payloadSizeStr = Environment.GetEnvironmentVariable("TTQL_PAYLOAD");
if (!string.IsNullOrWhiteSpace(payloadSizeStr))
    payloadSize = Convert.ToInt32(payloadSizeStr);

Console.WriteLine($"PAYLOAD\t{payloadSize}");

var dopPut = Math.Max(0, Convert.ToInt32(Environment.GetEnvironmentVariable("TTQL_DOP_PUT")));
Console.WriteLine($"DOP_PUT\t{dopPut}");

var dopGet = Math.Max(0, Convert.ToInt32(Environment.GetEnvironmentVariable("TTQL_DOP_GET")));
Console.WriteLine($"DOP_GET\t{dopGet}");

var putRps = new RpsMeter();
var getRps = new RpsMeter();

_ = Task.Factory.StartNew(async () =>
{
    var sem = new SemaphoreSlim(dopPut);
    var qm = new LQueue();
    var rnd = new Random();
    while (true)
    {
        await sem.WaitAsync();
        var start = DateTime.Now;
        var payload = string.Join('|', Enumerable.Range(0, payloadSize / 30 + 1).Select(i => Guid.NewGuid().ToString())).Substring(0, payloadSize);
        _ = qm.Put(new QueueMsg
        {
            id = Guid.NewGuid().ToString(),
            status = 0,
            qid = rnd.Next(N),
            routerTag = "new",
            vs = rnd.NextDouble() < 0.5 ? "vs" : "xs",
            messageType = 0,
            ts = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
            payload = payload
        }).ContinueWith(t =>
        {
            putRps.Hit(DateTime.Now - start);
            sem.Release();
        });
    }
}, TaskCreationOptions.LongRunning);

_ = Task.Factory.StartNew(async () =>
{

    var rnd = new Random();
    var sem = new SemaphoreSlim(dopGet);
    var qm = new LQueue();

    while (true)
    {
        await sem.WaitAsync();
        var start = DateTime.Now;
        var qid = rnd.Next(N);
        var vs = rnd.NextDouble() < 0.5 ? "vs" : null;
        _ = qm.Get(qid, "new", vs).ContinueWith(async t =>
         {
             var msg = t.Result;
             if (msg is not null)
                 await qm.Ack(msg.id);
             getRps.Hit(DateTime.Now - start);
             sem.Release();
         });
    }
}, TaskCreationOptions.LongRunning);

Console.WriteLine();
Console.WriteLine("     RPS\tLAT\t\t      RPS\tLAT");
while (true)
{
    await Task.Delay(1000);
    Console.WriteLine($"put: {putRps.GetRps():0}\t{putRps.GetLat():00.0}\t\t get: {getRps.GetRps():0}\t{getRps.GetLat():00.0}");
}

//while (true) await Task.Delay(10);
