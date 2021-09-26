using Microsoft.AspNetCore.Mvc;
using TTQ.Manager;

namespace TTQ.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TtqController : ControllerBase
    {
        readonly TtqAggregator qm;
        public TtqController(TtqAggregator manager) => qm = manager;

        [HttpPost("Put")] public async Task Put(QueueMsg msg) => await qm.Put(msg);
        [HttpGet("Get")] public async Task<QueueMsg> Get(int qid, string routerTag, string? vs = null) => await qm.Get(qid, routerTag, vs);
        [HttpGet("Ack")] public async Task Ack(string mid) => await qm.Ack(mid);

        //[HttpPost("Put")] public async Task Put(QueueMsg msg) { }// => await qm.Put(msg);
        //[HttpGet("Get")] public async Task<QueueMsg> Get(int qid, string routerTag, string? vs = null) => null; //await qm.Get(qid, routerTag, vs);
        //[HttpGet("Ack")] public async Task Ack(string mid) { } // => await qm.Ack(mid);
    }
}
