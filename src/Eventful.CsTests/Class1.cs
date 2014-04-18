using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using NUnit.Framework;

namespace Eventful.CsTests
{
    [TestFixture]
    public class Class1
    {
        [Test]
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

            Assert.That(count, Is.EqualTo(1));
        }
    }
}
