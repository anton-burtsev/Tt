using Tt;
using TTQ.Manager;

var tt = Environment.GetEnvironmentVariable("TTQ_TT");
if (string.IsNullOrWhiteSpace(tt)) tt = "localhost:3301";
Console.WriteLine($"TT\t{tt}");

var N = Math.Max(1, Convert.ToInt32(Environment.GetEnvironmentVariable("TTQ_N")));
Console.WriteLine($"QUEUES\t{N}");

var wait = Convert.ToInt32(Environment.GetEnvironmentVariable("TTQ_WAIT"));

var dopPut = 180;
var dopPutStr = Environment.GetEnvironmentVariable("TTQ_DOP_PUT");
if (!string.IsNullOrWhiteSpace(dopPutStr))
    dopPut = Convert.ToInt32(dopPutStr);

Console.WriteLine($"DOP_PUT\t{dopPut}");

var dopGet = 180;
var dopGetStr = Environment.GetEnvironmentVariable("TTQ_DOP_GET");
if (!string.IsNullOrWhiteSpace(dopGetStr))
    dopGet = Convert.ToInt32(dopGetStr);

Console.WriteLine($"DOP_GET\t{dopGet}");

var payloadSize = 50;
var payloadSizeStr = Environment.GetEnvironmentVariable("TTQ_PAYLOAD");
if (!string.IsNullOrWhiteSpace(payloadSizeStr))
    payloadSize = Convert.ToInt32(payloadSizeStr);

Console.WriteLine($"PAYLOAD\t{payloadSize}");


using var qm = new QueueManager();
await qm.Connect(tt);
await qm.Clear();

await qm.Put(new QueueMsg { id = Guid.Empty.ToString(), status = 0, qid = 0, routerTag = "none", vs = "vs", messageType = 0, payload = "ttq works fine" });
var msg = await qm.Get(0,"none");
Console.WriteLine(msg.payload);
await qm.Ack(Guid.Empty.ToString());

var putRps = new RpsMeter();
var getRps = new RpsMeter();

var total = 0L;
_ = Task.Factory.StartNew(async () => {
    var rnd = new Random();
    var sem = new SemaphoreSlim(dopPut);
    while (true)
    {
        await sem.WaitAsync();
        var start = DateTime.Now;
        var payload =  string.Join('|', Enumerable.Range(0, payloadSize/30+1).Select(i => Guid.NewGuid().ToString())).Substring(0, payloadSize);
        _ = qm.Put(new QueueMsg {
            id = Guid.NewGuid().ToString(),
            status = 0,
            qid = rnd.Next(N),
            routerTag = "new",
            vs = rnd.NextDouble() < 0.5 ? "vs" : "xs",
            messageType = 0,
            ts = DateTimeOffset.UtcNow.Ticks,
            payload = payload
        }).ContinueWith(t => {
            putRps.Hit(DateTime.Now - start);
            total++;
            sem.Release();
        });
    }
}, TaskCreationOptions.LongRunning);

while (total < wait)
{
    await Task.Delay(1000);
    Console.WriteLine(total);
}

_ = Task.Factory.StartNew(async () => {
    //return;
    var n = 0;
    var rnd = new Random();
    var sem = new SemaphoreSlim(dopGet);
    while (true)
    {
        await sem.WaitAsync();
        Interlocked.Increment(ref n);
        var start = DateTime.Now;
        var qid = rnd.Next(N);
        _ = qm.Get(qid, "new").ContinueWith(async t =>
        {
            var msg = t.Result;
            if (msg is not null)
                await qm.Ack(t.Result.id);
            getRps.Hit(DateTime.Now - start);
            sem.Release();
        });
    }
}, TaskCreationOptions.LongRunning);

_ = Task.Factory.StartNew(async () => {
    //return;
    var n = 0;
    var rnd = new Random();
    var sem = new SemaphoreSlim(dopGet);
    while (true)
    {
        await sem.WaitAsync();
        Interlocked.Increment(ref n);
        var start = DateTime.Now;
        var qid = rnd.Next(N);
        _ = qm.Get(qid, "new", "vs").ContinueWith(async t =>
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

