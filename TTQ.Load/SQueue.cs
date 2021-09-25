using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Channels;
using TTQ.Manager;

namespace TTQ.Load
{
    public class SQueue
    {
        readonly TcpClient client = new TcpClient();
        readonly SemaphoreSlim sem = new SemaphoreSlim(1);
        readonly Dictionary<Guid, TaskCompletionSource<QueueMsg>> wget = new();
        readonly Dictionary<Guid, TaskCompletionSource> wput = new();
        readonly Dictionary<Guid, TaskCompletionSource> wack = new();
        Channel<(byte[] header, byte[] message)> chin = Channel.CreateBounded<(byte[] header, byte[] message)>(10000);
        Channel<byte[]> chout = Channel.CreateBounded<byte[]>(10000);

        public async Task Connect(string hostname, int port)
        {
            await client.ConnectAsync(hostname, port);

            IoTools.RunAsyncIO(chout, chin, client.GetStream(), (header, message) =>
            {
                var resp = JsonSerializer.Deserialize<TtqResponse>(header);
                if (resp.Operation == TtqOpType.GET)
                {
                    var msg = message.Length > 0 ? JsonSerializer.Deserialize<QueueMsg>(message) : null;
                    lock (wget)
                        if (wget.TryGetValue(resp.RequestId, out var req))
                        {
                            wget.Remove(resp.RequestId);
                            req.SetResult(msg);
                        }
                }
                if (resp.Operation == TtqOpType.PUT)
                {
                    lock (wput)
                        if (wput.TryGetValue(resp.RequestId, out var req))
                        {
                            wput.Remove(resp.RequestId);
                            req.SetResult();
                        }
                }
                if (resp.Operation == TtqOpType.ACK)
                {
                    lock (wack)
                        if (wack.TryGetValue(resp.RequestId, out var req))
                        {
                            wack.Remove(resp.RequestId);
                            req.SetResult();
                        }
                }
            });
        }


        public async Task Put(QueueMsg msg)
        {
            var h = new TtqRequest { Operation = TtqOpType.PUT };
            var tcs = new TaskCompletionSource();
            lock (wput) wput[h.Id] = tcs;
            await chout.Writer.WriteAsync(IoTools.SerializePair(h, msg));
            await tcs.Task;
        }

        public async Task<QueueMsg> Get(int qid, string routerTag, string vs = null)
        {
            var h = new TtqRequest { Operation = TtqOpType.GET };
            var tcs = new TaskCompletionSource<QueueMsg>();
            lock (wget) wget[h.Id] = tcs;
            await chout.Writer.WriteAsync(IoTools.SerializePair(h, new GetRequest { qid = qid, routerTag = routerTag, vs = vs }));
            return await tcs.Task;
        }

        public async Task Ack(string mid)
        {
            var h = new TtqRequest { Operation = TtqOpType.ACK };
            var tcs = new TaskCompletionSource();
            lock (wack) wack[h.Id] = tcs;
            await chout.Writer.WriteAsync(IoTools.SerializePair(h, new AckRequest { mid = mid }));
            await tcs.Task;
        }
    }
}
