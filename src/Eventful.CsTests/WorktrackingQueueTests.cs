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
            var tcs = new TaskCompletionSource<bool>();
            tcs.SetResult(true);
            return tcs.Task;
        }

        public async Task Complete(string item)
        {
            Console.WriteLine("Complete");
        }

        [Fact]
        public async Task TestQueue()
        {
            Assert.Equal(0, _count);

            var queue = new CSharp.WorktrackingQueue<string, string>(
                i => new List<string> { i },
                DoWork,
                1000,
                1,
                Complete);

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

            
            Assert.True(_count > 1, "Callback should have been made at least once");
        }
    }
}
