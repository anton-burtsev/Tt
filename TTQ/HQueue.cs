using TTQ.Manager;

namespace TTQ
{
    public class TtqAggregator
    {
        readonly SQueue[] clients = new SQueue[Math.Max(1, Convert.ToInt32(Environment.GetEnvironmentVariable("TTQ_TT_N")))];
        public async Task Connect()
        {
            for (var i = 0; i < clients.Length; i++)
            {
                clients[i] = new();
                await clients[i].Connect($"tt-{i}.tt.tt.svc.cluster.local", 2020);
            }
        }

        public async Task Put(QueueMsg m) => await clients[m.GetShardNumber(clients.Length)].Put(m);

        Random rnd = new Random();
        public async Task<QueueMsg> Get(int qid, string routerTag, string vs = null)
        {
            var begin = rnd.Next(clients.Length);
            for (var i = 0; i < clients.Length; i++)
            {
                var m = await clients[(begin + i) % clients.Length].Get(qid, routerTag, vs);
                if (m is not null)
                    return m;
            }
            return null;
        }
        public async Task Ack(string mid) => await clients[QueueMsg.GetShardNumber(mid, clients.Length)].Ack(mid);
    }
}
