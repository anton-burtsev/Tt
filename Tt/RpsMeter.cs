using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tt
{
    public class RpsMeter
    {
        class V
        {
            public long mill;
            public int cnt;
        }
        class D
        {
            public TimeSpan duration;
            public int cnt;
        }

        readonly List<V> buckets = new() { new V { mill = 0, cnt = 0 } };
        readonly Stopwatch sw = Stopwatch.StartNew();
        int S = 0;
        readonly D[] avgs = new D[] { new(), new(), new(), new(), new() };
        int R = 0;

        public void Hit(TimeSpan duration)
        {
            lock (avgs)
            {
                var prev = R / 1000;
                R++;
                var cur = R / 1000;

                if (cur >= avgs.Length) { R = 0; cur = 0; }
                if (prev != cur)
                    avgs[cur] = new D();
                avgs[cur].cnt++;
                avgs[cur].duration += duration;
            }

            lock (buckets)
            {
                var i = sw.ElapsedMilliseconds;
                if (buckets[^1].mill == i)
                    buckets[^1].cnt++;
                else
                    buckets.Add(new V { mill = i, cnt = 1 });
                S++;
                while (buckets.Count > 1000)
                {
                    S -= buckets[0].cnt;
                    buckets.RemoveAt(0);
                }
            }
        }
        public double GetLat()
        {
            lock (avgs)
            {
                var total = avgs.Sum(a => a.cnt);
                if (total > 0)
                    return avgs.Sum(a => a.duration.TotalMilliseconds) / total;
                return 0;
            }
        }
        public double GetRps()
        {
            lock (buckets)
                return 1000.0 * S / (sw.ElapsedMilliseconds - buckets[0].mill);
        }
    }
}
