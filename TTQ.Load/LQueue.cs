using System.Net.Http.Json;
using System.Text.Json;
using TTQ.Manager;

namespace TTQ.Load
{
    public class LQueue
    {
        HttpClient[] clients = new HttpClient[Math.Max(1, Convert.ToInt32(Environment.GetEnvironmentVariable("TTQL_EP_N")))];
        public LQueue()
        {
            for (var i = 0; i < clients.Length; i++)
                clients[i] = new HttpClient { BaseAddress = new Uri($"http://ttq-{i}.ttq.tt.svc.cluster.local") };
        }
        public async Task Put(QueueMsg m) =>
            await clients[m.qid % clients.Length].PostAsync("/Ttq/Put", JsonContent.Create(m));

        public async Task<QueueMsg?> Get(int qid, string routerTag, string? vs = null)
        {
            var s = string.Empty;
            if (vs is null)
                s = await clients[qid % clients.Length].GetStringAsync($"/Ttq/Get?qid={qid}&routerTag={routerTag}");
            else
                s = await clients[qid % clients.Length].GetStringAsync($"/Ttq/Get?qid={qid}&routerTag={routerTag}&vs={vs}");

            if (string.IsNullOrWhiteSpace(s))
                return null;
            return JsonSerializer.Deserialize<QueueMsg?>(s);
        }

        Random rnd = new();
        public async Task Ack(string mid) =>
            await clients[rnd.Next(clients.Length)].GetAsync($"/Ttq/Ack?mid={mid}");
    }
}
