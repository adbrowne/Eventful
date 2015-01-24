using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Eventful.CsTests
{
    public class WorktrackingQueueTests
    {
        int _count;

        public void Blah(int foo = 3)
        {
            
        }
        public Task DoWork(string group, IEnumerable<string> items)
        {
            Console.WriteLine("Incrimenting");
            Interlocked.Increment(ref _count);
            Console.WriteLine("Incrimented");
            foreach (var item in items)
            {
                Console.WriteLine("DoWork {0}", item);
            }
            var tcs = new TaskCompletionSource<bool>();
            tcs.SetResult(true);
            return tcs.Task;
        }

#pragma warning disable 1998
        public async Task Complete(string item)
#pragma warning restore 1998
        {
            Console.WriteLine("Complete");
        }

        [Fact]
        [Trait("category", "unit")]
        public async Task TestQueue()
        {
            Assert.Equal(0, _count);

            var queue = new CSharp.WorktrackingQueue<string, string>(
                i => new List<string> { i },
                DoWork,
                1000,
                1,
                Complete);

            await queue.Add("0something");
            await queue.Add("1something");
            await queue.Add("2something");
            await queue.Add("3something");
            await queue.Add("4something");
            await queue.Add("5something");
            await queue.Add("6something");
            await queue.Add("7something");
            await queue.Add("8something");
            await queue.Add("9something");

            await queue.AsyncComplete();
            
            Assert.True(_count > 1, "Callback should have been made at least once");
        }
    }
}
