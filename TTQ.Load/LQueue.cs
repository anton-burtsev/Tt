using System.Net.Http.Json;
using System.Text.Json;
using TTQ.Manager;

namespace TTQ.Load
{
    public class LQueue
    {
        HttpClient client = new() { BaseAddress = new Uri("http://ttq.tt.svc.cluster.local") };
        public async Task Put(QueueMsg m) =>
            await client.PostAsync("/Ttq/Put", JsonContent.Create(m));

        public async Task<QueueMsg?> Get(int qid, string routerTag, string? vs = null)
        {
            var s = string.Empty;
            if (vs is null)
                s = await client.GetStringAsync($"/Ttq/Get?qid={qid}&routerTag={routerTag}");
            else
                s = await client.GetStringAsync($"/Ttq/Get?qid={qid}&routerTag={routerTag}&vs={vs}");

            if (string.IsNullOrWhiteSpace(s))
                return null;
            return JsonSerializer.Deserialize<QueueMsg?>(s);
        }

        public async Task Ack(string mid) =>
            await client.GetAsync($"/Ttq/Ack?mid={mid}");
    }
}
