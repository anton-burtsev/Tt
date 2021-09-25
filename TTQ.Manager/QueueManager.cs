﻿using ProGaudi.MsgPack.Light;
using ProGaudi.Tarantool.Client;
using ProGaudi.Tarantool.Client.Model;
using ProGaudi.Tarantool.Client.Model.Enums;
using ProGaudi.Tarantool.Client.Model.UpdateOperations;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;

namespace TTQ.Manager
{
    public class QueueManager : IDisposable
    {
        Box? box;
        ISpace space;
        IIndex i_queue_q;
        IIndex i_queue_q_vs;

        readonly Dictionary<MsgRequest, List<TaskCompletionSource<QueueMsg>>> requestMap = new();
        readonly Channel<MsgRequest> requestChannel = Channel.CreateUnbounded<MsgRequest>();

        public async Task Connect(string tt)
        {
            var msgPackContext = new MsgPackContext();
            msgPackContext.GenerateAndRegisterArrayConverter<QueueMsg>();
            var clientOptions = new ClientOptions(tt, context: msgPackContext);
            clientOptions.ConnectionOptions.WriteThrottlePeriodInMs = 0;


            box = new Box(clientOptions);
            await box.Connect();
            var s = box.GetSchema();
            space = s["QUEUE"];
            i_queue_q = space["QUEUE_Q"];
            i_queue_q_vs = space["QUEUE_Q_VS"];

            await box.Eval<int>($"box.cfg {{ memtx_memory = {16_000_000_000} }}");

            _ = Task.Factory.StartNew(async () =>
            {
                var sems = Enumerable.Range(0, 1000).Select(_ => new SemaphoreSlim(1)).ToArray();

                var sem = new SemaphoreSlim(2);

                while (true)
                {
                    await sem.WaitAsync();
                    _ = requestChannel.Reader.ReadAsync().AsTask().ContinueWith(async t => {
                        var iii = Math.Abs(t.Result.GetStaticHashCode()) % sems.Length;
                        await sems[iii].WaitAsync();
                        _ = processRequest(t.Result).ContinueWith(_ => {
                            sems[iii].Release();
                            sem.Release();
                        });
                    });
                }
            });
        }

        async Task processRequest(MsgRequest req)
        {
            try
            {
                List<TaskCompletionSource<QueueMsg>> tasks;
                lock (requestMap)
                    if (requestMap.TryGetValue(req, out tasks))
                        requestMap.Remove(req);

                //if (tasks is not null)
                //    for (var i = 0; i < tasks.Count; i++)
                //        tasks[i].SetResult(null);
                //return;

                if (tasks is null) return;

                //var sw = Stopwatch.StartNew();

                QueueMsg[] recs;
                if ( string.IsNullOrWhiteSpace(req.vs))
                {
                    recs = (await i_queue_q_vs.Select<(long, long, string), QueueMsg>(
                        (0, req.qid, req.routerTag), new SelectOptions { Iterator = Iterator.Eq, Limit = (uint)tasks.Count })).Data;
                }
                else
                {
                    recs = (await i_queue_q_vs.Select<(long, long, string, string), QueueMsg>(
                        (0, req.qid, req.routerTag, req.vs), new SelectOptions { Iterator = Iterator.Eq, Limit = (uint)tasks.Count })).Data;
                }

                //Console.WriteLine(sw.ElapsedMilliseconds);

                if (recs.Length > 0)
                {
                    var n = 0;
                    var tcs = new TaskCompletionSource();
                    for (var i = 0; i < recs.Length; i++)
                        _ = SetStatus(recs[i].id, 1).ContinueWith(t =>
                        {
                            if (Interlocked.Increment(ref n) == recs.Length) tcs.SetResult();
                        });

                    await tcs.Task;
                }


                for (var i = 0; i < tasks.Count; i++)
                    if (i < recs.Length)
                        tasks[i].SetResult(recs[i]);
                    else
                        tasks[i].SetResult(null);

                //Console.WriteLine($"count: {tasks.Count}\tlat: {sw.ElapsedMilliseconds}");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        async Task SetStatus(string id, long status) =>
            await space.Update<ValueTuple<string>, QueueMsg>(ValueTuple.Create(id), new[] { UpdateOperation.CreateAssign(1, status) });

        public async Task Ack(string id) => await space.Delete<ValueTuple<string>, QueueMsg>(ValueTuple.Create(id));

        public async Task<QueueMsg> Get2(long qid, string routerTag, string vs = null)
        {
            QueueMsg[] recs;
            if (string.IsNullOrWhiteSpace(vs))
            {
                recs = (await i_queue_q_vs.Select<(long, long, string), QueueMsg>(
                    (0, qid, routerTag), new SelectOptions { Iterator = Iterator.Eq, Limit = 1 })).Data;
            }
            else
            {
                recs = (await i_queue_q_vs.Select<(long, long, string, string), QueueMsg>(
                    (0, qid, routerTag, vs), new SelectOptions { Iterator = Iterator.Eq, Limit = 1 })).Data;
            }


            if (recs.Length > 0)
            {
                await SetStatus(recs[0].id, 1);

                return recs[0];
            }
            return null;
        }
        public async Task<QueueMsg> Get(long qid, string routerTag, string vs = null)
        {
            var t = new TaskCompletionSource<QueueMsg>();
            var push = false;
            var req = new MsgRequest { qid = qid, routerTag = routerTag, vs = vs };
            lock (requestMap)
            {
                if (requestMap.TryGetValue(req, out var tasks))
                    tasks.Add(t);
                else
                {
                    requestMap[req] = new List<TaskCompletionSource<QueueMsg>> { t };
                    push = true;
                }
            }

            if (push)
                await requestChannel.Writer.WriteAsync(req);

            return await t.Task;
        }

        public async Task Put(QueueMsg msg) => await space.Insert(msg);

        public void Dispose()
        {
            if (box is not null)
                box.Dispose();
        }

        public async Task Clear() => await box.ExecuteSql("truncate table QUEUE");
    }
    public class QueueMsg
    {
        [MsgPackArrayElement(0)]
        public string id { get; set; }
        [MsgPackArrayElement(1)]
        public int status { get; set; }
        [MsgPackArrayElement(2)]
        public long qid { get; set; }
        [MsgPackArrayElement(3)]
        public string? routerTag { get; set; }
        [MsgPackArrayElement(4)]
        public string? vs { get; set; }
        [MsgPackArrayElement(5)]
        public long messageType { get; set; }
        [MsgPackArrayElement(6)]
        public long ts { get; set; }
        [MsgPackArrayElement(7)]
        public string? payload { get; set; }

        public static int GetShardNumber(string mid, int totalShards) => Math.Abs(mid.GetHashCode()) % totalShards;
        public int GetShardNumber(int totalShards) => GetShardNumber(id, totalShards);
    }

    public class MsgRequest
    {
        public long qid { get; set; }
        public string routerTag { get; set; }
        public string vs { get; set; }
        public int messageType { get; set; }

        public override bool Equals(object? obj) => (obj is MsgRequest msg) && qid == msg.qid && routerTag == msg.routerTag && vs == msg.vs && messageType == msg.messageType;

        public override int GetHashCode() => $"{qid}|{vs}|{routerTag}|{messageType}".GetHashCode();
        public int GetStaticHashCode() => $"{qid}|{routerTag}".GetHashCode();
    }

    public enum TtqOpType { NOP,PUT,GET,ACK }
    public class TtqRequest
    {
        public TtqOpType Operation { get; set; }
        public Guid Id { get; set; } = Guid.NewGuid();
    }
    public class TtqResponse
    {
        public TtqOpType Operation { get; set; }
        public Guid RequestId { get; set; }
    }

    public class GetRequest
    {
        public int qid { get; set; }
        public string routerTag { get; set; }
        public string vs { get; set; }
    }
    public class AckRequest
    {
        public string mid { get; set; }
    }



    public static class IoTools
    {

        public static byte[] SerializePair(object h, object m = null)
        {
            var ms = new MemoryStream();
            var bw = new BinaryWriter(ms);

            var h_data = JsonSerializer.SerializeToUtf8Bytes(h);
            bw.Write(h_data.Length);
            bw.Write(h_data);

            if (m is not null)
            {
                var m_data = JsonSerializer.SerializeToUtf8Bytes(m);
                bw.Write(m_data.Length);
                bw.Write(m_data);
            }
            else
                bw.Write(0);

            return ms.ToArray();
        }

        public static void RunAsyncIO(Channel<byte[]> chout, Channel<(byte[], byte[])> chin, Stream s, Action<byte[], byte[]> processor)
        {
            _ = Task.Factory.StartNew(async () => // read all data from network
            {
                var br = new BinaryReader(new BufferedStream(s, 1024 * 1024));
                while (true)
                {
                    await chin.Writer.WriteAsync((br.ReadBytes(br.ReadInt32()), br.ReadBytes(br.ReadInt32())));
                }
            }, TaskCreationOptions.LongRunning);

            _ = Task.Factory.StartNew(async () => // write all data to network
            {
                var bw = new BinaryWriter(new BufferedStream(s, 1024 * 1024));
                await foreach (var b in chout.Reader.ReadAllAsync())
                {
                    bw.Write(b);
                    if (chout.Reader.Count == 0)
                        bw.Flush();
                }
            }, TaskCreationOptions.LongRunning);

            _ = Task.Factory.StartNew(async () => // process message 1 by 1
            {
                await foreach (var (header, message) in chin.Reader.ReadAllAsync())
                    processor(header, message);
            });
        }
    }
}