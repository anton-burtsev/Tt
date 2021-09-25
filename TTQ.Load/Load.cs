using System.Net;
using System.Net.Http.Json;
using TTQ.Load;
using TTQ.Manager;

var N = Math.Max(1, Convert.ToInt32(Environment.GetEnvironmentVariable("TTQL_N")));
Console.WriteLine($"QUEUES\t{N}");

var payloadSize = 50;
var payloadSizeStr = Environment.GetEnvironmentVariable("TTQL_PAYLOAD");
if (!string.IsNullOrWhiteSpace(payloadSizeStr))
    payloadSize = Convert.ToInt32(payloadSizeStr);

Console.WriteLine($"PAYLOAD\t{payloadSize}");

var dopPut = Math.Max(1, Convert.ToInt32(Environment.GetEnvironmentVariable("TTQL_DOP_PUT")));
Console.WriteLine($"DOP_PUT\t{dopPut}");

var dopGet = Math.Max(1, Convert.ToInt32(Environment.GetEnvironmentVariable("TTQL_DOP_GET")));
Console.WriteLine($"DOP_GET\t{dopGet}");


var bhost = Environment.GetEnvironmentVariable("TTQL_BHOST");
if (string.IsNullOrWhiteSpace(payloadSizeStr))
    bhost = "localhost";

Console.WriteLine($"BHOST\t{bhost}");

//var s = new SQueue();
//await s.Connect("localhost", 2020);

//await s.Put(new QueueMsg
//{
//    id = Guid.NewGuid().ToString(),
//    status = 0,
//    qid = 1111,
//    routerTag = "new",
//    vs = "xs",
//    messageType = 0,
//    ts = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
//    payload = "payload"
//});

//var mmm = await s.Get(1111, "new");
//if (mmm is null)
//    Console.WriteLine("[null]");
//Console.WriteLine($"x: {mmm}");
//Console.WriteLine(mmm.payload);
//await s.Ack(mmm.id);

//Console.ReadLine();
//return;

//Console.ReadLine();

//dopPut = 0;
//dopGet = 100;
//N = 100;

//_ = Task.Factory.StartNew(async () => {
//    return;
//    var l = new LQueue();
//    var sem = new SemaphoreSlim(1);
//    var rnd = new Random();

//    while (true)
//    {
//        await sem.WaitAsync();
//        var start = DateTime.Now;
//        var qid = rnd.Next(N);
//        var vs = rnd.NextDouble() < 0.5 ? "vs" : null;

//        //Console.WriteLine(qid);
//        _ = l.Get(0, "new", "vs").ContinueWith(async t =>
//        {
//            var msg = t.Result;
//            if (msg is not null)
//                await l.Ack(msg.id);

//            getRps.Hit(DateTime.Now - start);
//            sem.Release();
//        });

//        Console.WriteLine(Guid.NewGuid());
//    }

//}, TaskCreationOptions.LongRunning);

//Console.ReadLine();

var putRps = new RpsMeter();
var getRps = new RpsMeter();

dopPut = 200;
dopGet = 7000;


_ = Task.Factory.StartNew(async () => {
    var sem = new SemaphoreSlim(dopPut);
    var qm = new SQueue();
    await qm.Connect(bhost, 2020);
    var rnd = new Random();
    while (true)
    {
        await sem.WaitAsync();
        var start = DateTime.Now;
        var payload = string.Join('|', Enumerable.Range(0, payloadSize / 30 + 1).Select(i => Guid.NewGuid().ToString())).Substring(0, payloadSize);
        _=qm.Put(new QueueMsg
        {
            id = Guid.NewGuid().ToString(),
            status = 0,
            qid = rnd.Next(N),
            routerTag = "new",
            vs = rnd.NextDouble() < 0.5 ? "vs" : "xs",
            messageType = 0,
            ts = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
            payload = payload
        }).ContinueWith(t => {
            putRps.Hit(DateTime.Now - start);
            sem.Release();
        });
    }
}, TaskCreationOptions.LongRunning);


_ = Task.Factory.StartNew(async () => {

    var rnd = new Random();
    var sem = new SemaphoreSlim(dopGet);
    var qm = new SQueue();
    await qm.Connect(bhost, 2020);

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

while (true) await Task.Delay(10);
