using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Eventful.CsTests
{
    public class ParellelInOrderTransformerTest
    {
        [Fact]
        [Trait("category", "unit")]
        public async Task CanRunItemsInParallel()
        {
            var rnd = new Random();

            var tranformer = new CSharp.ParallelInOrderTransformer<int, int>(v =>
                {
                    Thread.Sleep(rnd.Next(10));
                    return v;
                }, 50, 5);

            var list = new List<int>();

            const int itemCount = 10;
            foreach(var i in Enumerable.Range(1, itemCount))
            {
                tranformer.Process(i, result =>
                    {
                        Thread.Sleep(rnd.Next(10));
                        lock (list)
                        {
                            list.Add(result);
                        }
                    });
            }

            while (list.Count < itemCount)
            {
                await Task.Delay(100);
            }

            Assert.Equal(itemCount, list.Count);
            var zippedList = list.Zip(Enumerable.Range(1, itemCount), (a, b) => a == b);
            foreach (var item in zippedList)
            {
                Assert.True(item);
            }
        }
    }
}
