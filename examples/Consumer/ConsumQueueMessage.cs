using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Consumer
{
    public class ConsumQueueMessage<TKey, TValue>
    {
        string _topic;
        readonly ConsumerConfig _conf;
        readonly Func<ConsumeResult<TKey, TValue>, Task> _handleBusiness;
        readonly Channel<List<ConsumeResult<TKey, TValue>>> _queue;
        IConsumer<TKey, TValue> _consumer;
        int _batchSize;

        public IConsumer<TKey, TValue> Consumer => _consumer;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="conf"></param>
        /// <param name="handleBusiness"></param>
        /// <param name="topic"></param>
        /// <param name="capacity">内部消息队列容量,建议不要超过50</param>
        /// <param name="batchSize">每批次消息数量</param>
        public ConsumQueueMessage(ConsumerConfig conf, Func<ConsumeResult<TKey, TValue>, Task> handleBusiness, string topic, int capacity,int batchSize = 1)
        {
            if (conf == null) throw new ArgumentNullException("消费者配置不能为空");
            if (string.IsNullOrEmpty(topic)) throw new ArgumentNullException("topic不能为空");

            _topic = topic;

            if (capacity <= 0)
                _queue = Channel.CreateUnbounded<List<ConsumeResult<TKey, TValue>>>();
            else
                _queue = Channel.CreateBounded<List<ConsumeResult<TKey, TValue>>>(new BoundedChannelOptions(capacity)
                {
                    Capacity = capacity,
                    FullMode = BoundedChannelFullMode.Wait
                });

            _conf = conf;
            _conf.EnableAutoCommit = false;//关闭自动更新(防止丢失数据)
            _conf.SocketKeepaliveEnable = true;
            _conf.EnablePartitionEof = true;
            _conf.SessionTimeoutMs = 30000;
            _handleBusiness = handleBusiness;
            _batchSize = batchSize;
        }

        /// <summary>
        /// 处理Channel数据
        /// </summary>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        public async Task ConsumeData(CancellationToken stoppingToken)
        {
            try
            {
                while (await _queue.Reader.WaitToReadAsync(stoppingToken))
                {
                    if (_queue.Reader.TryRead(out var listMsg))
                    {
                        if (listMsg != null && _handleBusiness != null && listMsg.Count > 0)
                        {
                            ConsumeResult<TKey, TValue> commitResult = null;
                            try
                            {
                                foreach(var msg in listMsg)
                                {
                                    await _handleBusiness(msg);
                                    commitResult = msg;
                                }                               
                            }
                            catch (KafkaException e)
                            {
                                Console.WriteLine($"Commit error: {e.Error.Reason}");
                            }
                            catch (Exception e1)
                            {
                                Console.WriteLine($"Commit error: {e1}");
                            }
                            finally
                            {
                                if (commitResult != null && _consumer != null)
                                {
                                    _consumer.Commit(commitResult);
                                }
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException ex)
            {
                Console.WriteLine($"服务被取消了!准备进行清理工作.at time:{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");
                while (await _queue.Reader.WaitToReadAsync())
                {
                    if (_queue.Reader.TryRead(out var listMsg))
                    {
                        if (listMsg != null && _handleBusiness != null && listMsg.Count > 0)
                        {
                            foreach (var msg in listMsg)
                            {
                                await _handleBusiness(msg);
                                _consumer.Commit(msg);
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 拉取kafka数据
        /// </summary>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        public async Task PullDataAsync(CancellationToken stoppingToken)
        {
            await Task.Run(async () =>
            {
                var builder = new ConsumerBuilder<TKey, TValue>(_conf);
                builder.SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                // .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
                    // possibly manually specify start offsets or override the partition assignment provided by
                    // the consumer group by returning a list of topic/partition/offsets to assign to, e.g.:
                    // 
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]");
                })
                ;
                using (_consumer = builder.Build())
                {
                    _consumer.Subscribe(_topic);//订阅topic
                        try
                    {
                        while (!stoppingToken.IsCancellationRequested)
                        {
                            try
                            {
                                var consumeResultList = _consumer.ConsumeBatch(stoppingToken, _batchSize );
                                if(consumeResultList != null && consumeResultList.Count > 0)
                                {
                                    await _queue.Writer.WriteAsync(consumeResultList, stoppingToken);
                                }
                               
                            }
                            catch (OperationCanceledException e)
                            {
                                Console.WriteLine($"服务被取消了", $"拉取数据结束:{_topic} at time:{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");
                                    _queue.Writer.Complete();
                                break;
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine($"消费数据被取消了,拉取数据结束:{_topic} at time:{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");
                            }
                        }
                    }
                    catch (OperationCanceledException e)
                    {
                        _queue.Writer.Complete();
                        Console.WriteLine($"服务已被取消-停止获取数据 at time:{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");
                    }
                }
            }, stoppingToken);
        }
    }


}
