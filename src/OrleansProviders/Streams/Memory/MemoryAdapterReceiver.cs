using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Streams;

namespace Orleans.Providers.Streams.Memory
{
    internal class MemoryAdapterReceiver : IQueueAdapterReceiver
    {
        private IMemoryStreamQueueGrain queueGrain;
        private long sequenceId;
        private Task awaitingTask;

        public MemoryAdapterReceiver(IMemoryStreamQueueGrain queueGrain)
        {
            this.queueGrain = queueGrain;
        }

        public Task Initialize(TimeSpan timeout)
        {
            return TaskDone.Done;
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            IEnumerable<MemoryEventData> eventData;
            List<IBatchContainer> batches;
            try
            {
                var task = queueGrain.Dequeue(maxCount);
                awaitingTask = task;
                eventData = await task;
                batches = eventData.Select(data => (IBatchContainer) MemoryBatchContainer.FromMemoryEventData<object>(data, ++sequenceId)).ToList();
                return batches;
            }
            catch (Exception exc)
            {
                throw;
            }
            finally
            {
                awaitingTask = null;
            }
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            return TaskDone.Done;
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            if (awaitingTask != null)
            {
                await awaitingTask;
            }
        }
    }
}
