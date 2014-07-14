using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Eventful.CsTests
{
    public class ParellelInOrderTransformerTest
    {
        [Fact]
        public async Task CanRunItemsInParallel()
        {
            var rnd = new Random();

            var tranformer = new CSharp.ParallelInOrderTransformer<int, int>(async v =>
                {
                    await Task.Delay(rnd.Next(10));
                    return v;
                }, 50, 5);

            var list = new List<int>();

            foreach(var i in Enumerable.Range(1, 100))
            {
                tranformer.Process(i, async result =>
                    {
                        await Task.Delay(rnd.Next(10));
                        lock (list)
                        {
                            list.Add(result);
                        }
                    });
            }

            while (list.Count < 100)
            {
                await Task.Delay(100);
            }

            Assert.Equal(100, list.Count);
            foreach (var item in list.Zip(Enumerable.Range(1, 100), (a, b) => a == b))
            {
                Assert.True(item);
            }
        }
    }
}
