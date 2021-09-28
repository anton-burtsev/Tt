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

var writer = new Writer("default.message");

_ = Task.Factory.StartNew(async () =>
{
    var sem = new SemaphoreSlim(dopPut);
    var qm = new LQueue();
    var rnd = new Random();
    var ramp = 30L;
    while (true)
    {
        if (ramp-- > 0) await Task.Delay((int)ramp);
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
            writer.Write(DateTime.Now, "PUT", (DateTime.Now - start).TotalMilliseconds, 1);
            sem.Release();
        });
    }
}, TaskCreationOptions.LongRunning);

_ = Task.Factory.StartNew(async () =>
{

    var rnd = new Random();
    var sem = new SemaphoreSlim(dopGet);
    var qm = new LQueue();
    var ramp = 30L;

    while (true)
    {
        if (ramp-- > 0) await Task.Delay((int)ramp);
        await sem.WaitAsync();
        var start = DateTime.Now;
        var qid = rnd.Next(N);
        var vs = rnd.NextDouble() < 0.5 ? "vs" : null;
        _ = qm.Get(qid, "new", vs).ContinueWith(async t =>
         {
             var msg = t.Result;
             writer.Write(DateTime.Now, "GET", (DateTime.Now - start).TotalMilliseconds, 1);
             start = DateTime.Now;
             if (msg is not null && !string.IsNullOrWhiteSpace(msg.id))
             {
                 await qm.Ack(msg.id);
                 writer.Write(DateTime.Now, "ACK", (DateTime.Now - start).TotalMilliseconds, 1);
             }
             sem.Release();
         });
    }
}, TaskCreationOptions.LongRunning);

while (true) await Task.Delay(100);
