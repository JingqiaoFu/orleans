using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Providers.Streams.Common;
using Orleans.Providers.Streams.Memory;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers
{
    /// <summary>
    /// Adapter factory for in memory stream provider.
    /// This factory acts as the adapter and the adapter factory.  The events are stored in an in-memory grain that 
    /// behaves as an event queue, this provider adapter is primarily used for testing
    /// </summary>
    public class MemoryAdapterFactory : IQueueAdapterFactory, IQueueAdapter, IQueueAdapterCache
    {
        private IStreamQueueMapper streamQueueMapper;
        private ConcurrentDictionary<QueueId, MemoryAdapterReceiver> receivers;
        private ConcurrentDictionary<QueueId, IMemoryStreamQueueGrain> queueGrains;
        private IObjectPool<FixedSizeBuffer> bufferPool;
        private IStreamFailureHandler streamFailureHandler;
        private IServiceProvider serviceProvider;
        private MemoryAdapterConfig adapterConfig;
        private Logger logger;

        public string Name { get { return adapterConfig.StreamProviderName; } }
        public bool IsRewindable { get { return false; } }
        public StreamProviderDirection Direction { get { return StreamProviderDirection.ReadOnly; } }

        protected Func<string, Task<IStreamFailureHandler>> StreamFailureHandlerFactory { get; set; }

        public void Init(IProviderConfiguration providerConfig, string providerName, Logger log, IServiceProvider svcProvider)
        {
            logger = log;
            serviceProvider = svcProvider;
            receivers = new ConcurrentDictionary<QueueId, MemoryAdapterReceiver>();
            queueGrains = new ConcurrentDictionary<QueueId, IMemoryStreamQueueGrain>();
            adapterConfig = new MemoryAdapterConfig(providerName);

            adapterConfig.PopulateFromProviderConfig(providerConfig);
            streamQueueMapper = new HashRingBasedStreamQueueMapper(adapterConfig.TotalQueueCount, adapterConfig.StreamProviderName);

            // 10 meg buffer pool.  10 1 meg blocks
            bufferPool = new FixedSizeObjectPool<FixedSizeBuffer>(10, pool => new FixedSizeBuffer(1 << 20, pool));
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            return Task.FromResult<IQueueAdapter>(this);
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return this;
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return streamQueueMapper;
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            IMemoryStreamQueueGrain grain = queueGrains.GetOrAdd(queueId, ((GrainFactory)serviceProvider.GetService(typeof(GrainFactory))).GetGrain<IMemoryStreamQueueGrain>(Guid.NewGuid()));
            IQueueAdapterReceiver receiver = (IQueueAdapterReceiver)receivers.GetOrAdd(queueId, qid => new MemoryAdapterReceiver(queueGrains.GetOrAdd(queueId, grain)));
            return receiver;
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            QueueId queueId;
            MemoryEventData eventData;
            try
            {
                queueId = streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
                eventData = MemoryBatchContainer.ToMemoryEventData(streamGuid, streamNamespace, events, requestContext);
                CreateReceiver(queueId);
                await queueGrains[queueId].Enqueue(eventData);
            }
            catch (Exception exc)
            {
                logger.Error((int)ProviderErrorCode.ProvidersBase, "Exception thrown in MemoryAdapterFactory.QueueMessageBatchAsync.", exc);
                throw;
            }
        }

        public IQueueCache CreateQueueCache(QueueId queueId)
        {
            return new MemoryPooledCache(bufferPool);
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return Task.FromResult(streamFailureHandler ?? (streamFailureHandler = new NoOpStreamDeliveryFailureHandler()));
        }
    }
}
