using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using TTQ.Manager;

QueueManager qm = new QueueManager();
await qm.Connect("localhost:3301");


var tcpIn = new Dictionary<Guid,TcpClient>();
var tcpOut = new Dictionary<Guid,TcpClient>();

var server = new TcpListener(IPAddress.Any, 2020);
server.Start();

while (true)
{
    var client = await server.AcceptTcpClientAsync();

    _ = Task.Factory.StartNew(async () =>
    {
        try
        {
            var br = new BinaryReader(client.GetStream());
            var bw = new BinaryWriter(client.GetStream());

            var direction = br.ReadByte();
            var channelId = new Guid(br.ReadBytes(16));
            bw.Write((byte)3);

            if (direction == 1)
                lock (tcpIn)
                    tcpIn[channelId] = client;

            if (direction == 2)
                lock (tcpOut)
                    tcpOut[channelId] = client;

            var b_out = tcpOut.TryGetValue(channelId, out var tcp_out);
            var b_in = tcpIn.TryGetValue(channelId, out var tcp_in);

            if (b_out && b_in)
            {
                var chin = Channel.CreateBounded<(byte[] header, byte[] message)>(10000);
                var chout = Channel.CreateBounded<byte[]>(10000);

                IoTools.RunAsyncIO(chout, chin, tcp_out, tcp_in, (header, message) =>
                {
                    var request = IoTools.Deserialize<TtqRequest>(header);

                    if (request.Operation == TtqOpType.PUT)
                    {
                        var msg = IoTools.Deserialize<QueueMsg>(message);
                        _ = qm.Put(msg).ContinueWith(async t =>
                        {
                            await chout.Writer.WriteAsync(IoTools.SerializePair(
                                new TtqResponse { RequestId = request.Id, Operation = TtqOpType.PUT }));
                        });
                    }
                    if (request.Operation == TtqOpType.GET)
                    {
                        var getReq = IoTools.Deserialize<GetRequest>(message);
                        _ = qm.Get(getReq.qid, getReq.routerTag, getReq.vs).ContinueWith(async t =>
                        {
                            await chout.Writer.WriteAsync(IoTools.SerializePair(
                                new TtqResponse { RequestId = request.Id, Operation = TtqOpType.GET },
                                t.Result));
                        });
                    }
                    if (request.Operation == TtqOpType.ACK)
                    {
                        var ackReq = IoTools.Deserialize<AckRequest>(message);
                        _ = qm.Ack(ackReq.mid).ContinueWith(async t =>
                        {
                            await chout.Writer.WriteAsync(IoTools.SerializePair(
                                new TtqResponse { RequestId = request.Id, Operation = TtqOpType.ACK }));
                        });
                    }
                });
            }
        }
        catch (Exception ex) { Console.WriteLine(ex); }

    }, TaskCreationOptions.LongRunning);

}