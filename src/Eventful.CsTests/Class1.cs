using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.FSharp.Collections;
using Xunit;

namespace Eventful.CsTests
{
    public class Class1
    {
        [Fact]
        public async void TestQueue()
        {
            int count = 0;
            var queue = new WorktrackingQueueCs<string, string>(100, SetModule.Singleton,
                                                                s => Task.Run(() =>
                                                                    {
                                                                        Console.WriteLine("Complete");
                                                                    }), 
                                                                10,
                                                                (s, enumerable) =>
                                                                Task.Run(() => { 
                                                                    Interlocked.Increment(ref count);
                                                                    Console.WriteLine("Incrimented");
                                                                }));

            await queue.Add("something");
            await queue.Add("something");
            await queue.Add("something");
            await queue.Add("something");
            await queue.Add("something");
            await queue.Add("something");
            await queue.Add("something");
            await queue.Add("something");
            await queue.Add("something");
            await queue.Add("something");
            await queue.Add("something");
            await queue.Add("something");
            await queue.Add("something");

            await queue.AsyncComplete();

            Assert.Equal(1, count);
        }
    }
}
