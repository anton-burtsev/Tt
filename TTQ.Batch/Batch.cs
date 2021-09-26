using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Channels;
using TTQ.Manager;

var server = new TcpListener(IPAddress.Any, 2020);
server.Start();

await Task.Delay(3000);

QueueManager qm = new QueueManager();
await qm.Connect("localhost:3301");

while (true)
{
    var client = await server.AcceptTcpClientAsync();

    var chin = Channel.CreateBounded<(byte[] header, byte[] message)>(10000);
    var chout = Channel.CreateBounded<byte[]>(10000);

    IoTools.RunAsyncIO(chout, chin, client.GetStream(), (header, message) =>
    {
    var request = JsonSerializer.Deserialize<TtqRequest>(header);

    if (request.Operation == TtqOpType.PUT)
    {
        var msg = JsonSerializer.Deserialize<QueueMsg>(message);
            //Task.Factory.StartNew(async() =>
            //{
            //    await chout.Writer.WriteAsync(IoTools.SerializePair(
            //        new TtqResponse { RequestId = request.Id, Operation = TtqOpType.PUT }));
            //});

            _ = qm.Put(msg).ContinueWith(async t =>
              {
                  await chout.Writer.WriteAsync(IoTools.SerializePair(
                      new TtqResponse { RequestId = request.Id, Operation = TtqOpType.PUT }));
              });
        }
        if (request.Operation == TtqOpType.GET)
        {
            var getReq = JsonSerializer.Deserialize<GetRequest>(message);
            _ = qm.Get(getReq.qid, getReq.routerTag, getReq.vs).ContinueWith(async t =>
            {
                await chout.Writer.WriteAsync(IoTools.SerializePair(
                    new TtqResponse { RequestId = request.Id, Operation = TtqOpType.GET },
                    t.Result));
            });
        }
        if (request.Operation == TtqOpType.ACK)
        {
            var ackReq = JsonSerializer.Deserialize<AckRequest>(message);
            _ = qm.Ack(ackReq.mid).ContinueWith(async t =>
            {
                await chout.Writer.WriteAsync(IoTools.SerializePair(
                    new TtqResponse { RequestId = request.Id, Operation = TtqOpType.ACK }));
            });
        }
    });
}