using Confluent.Kafka;
using DW.KafkaViwer.Web.Models;

namespace DW.KafkaViwer.Web.Services.Kafka
{
    public partial class KafkaService
    {
        public List<TopicMessageInfo> GetTopicMessages(TopicMessageFilter filter)
        {
            // Find broker by ID
            var broker = _brokers[filter.BrokerId];
            if (broker == null)
            {
                throw new ArgumentException($"Broker with ID {filter.BrokerId} not found");
            }

            if (broker.Status != "Active")
            {
                throw new InvalidOperationException($"Broker {broker.ConnectionName} is not active");
            }

            var messages = new List<TopicMessageInfo>();

            try
            {
                // Get metadata using AdminClient
                var adminConfig = CreateAdminClientConfig(broker);
                using var adminClient = new AdminClientBuilder(adminConfig).Build();
                var metadata = adminClient.GetMetadata(filter.TopicName, TimeSpan.FromSeconds(10));
                var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == filter.TopicName);

                if (topicMetadata == null)
                {
                    throw new ArgumentException($"Topic '{filter.TopicName}' not found in broker {broker.ConnectionName}");
                }

                var partitions = topicMetadata.Partitions.Select(p => new TopicPartition(filter.TopicName, p.PartitionId)).ToList();

                // Determine time boundaries
                var hasTimeFilter = filter.From.HasValue || filter.To.HasValue;
                var endTime = filter.To ?? DateTime.UtcNow;
                var startTime = filter.From ?? DateTime.MinValue;
                var readTimeout = TimeSpan.FromSeconds(1);
                var totalLimit = filter.Limit ?? 1000;

                // Get watermark offsets and time-based offsets for all partitions
                var partitionOffsets = new Dictionary<int, WatermarkOffsets>();
                var partitionStartOffsets = new Dictionary<int, long>();
                var partitionEndOffsets = new Dictionary<int, long>();
                
                foreach (var partition in partitions)
                {
                    try
                    {
                        var tempConfig = CreateConsumerConfig(broker, $"watermark-{Guid.NewGuid()}");
                        using var tempConsumer = new ConsumerBuilder<Ignore, Ignore>(tempConfig).Build();
                        tempConsumer.Assign(partition);
                        Thread.Sleep(100);
                        
                        var watermarks = tempConsumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10));
                        partitionOffsets[partition.Partition] = watermarks;
                        
                        // Determine start and end offsets based on time filters and search type
                        long startOffset = watermarks.Low;
                        long endOffset = watermarks.High;
                        
                        // If time filter is specified, find offsets by timestamp using OffsetsForTimes
                        if (hasTimeFilter)
                        {
                            var timestampsToQuery = new List<TopicPartitionTimestamp>();
                            
                            if (filter.From.HasValue)
                            {
                                timestampsToQuery.Add(new TopicPartitionTimestamp(partition, new Timestamp(filter.From.Value, TimestampType.CreateTime)));
                            }
                            
                            if (filter.To.HasValue)
                            {
                                timestampsToQuery.Add(new TopicPartitionTimestamp(partition, new Timestamp(filter.To.Value, TimestampType.CreateTime)));
                            }
                            
                            if (timestampsToQuery.Count > 0)
                            {
                                var offsetsForTimes = tempConsumer.OffsetsForTimes(timestampsToQuery, TimeSpan.FromSeconds(10));
                                
                                // Match results with queries by partition
                                foreach (var queryTimestamp in timestampsToQuery)
                                {
                                    var result = offsetsForTimes.FirstOrDefault(o => o.Partition == queryTimestamp.Partition);
                                    if (result != null && result.Offset >= 0)
                                    {
                                        // Check if this is the From timestamp
                                        if (filter.From.HasValue)
                                        {
                                            var fromTimestamp = new Timestamp(filter.From.Value, TimestampType.CreateTime);
                                            if (queryTimestamp.Timestamp.UnixTimestampMs == fromTimestamp.UnixTimestampMs)
                                            {
                                                startOffset = result.Offset;
                                            }
                                        }
                                        
                                        // Check if this is the To timestamp
                                        if (filter.To.HasValue)
                                        {
                                            var toTimestamp = new Timestamp(filter.To.Value, TimestampType.CreateTime);
                                            if (queryTimestamp.Timestamp.UnixTimestampMs == toTimestamp.UnixTimestampMs)
                                            {
                                                endOffset = result.Offset;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            // No time filter - use SearchType to determine starting point
                            if (filter.SearchType == "newest" || filter.SearchType == "onlyNew")
                            {
                                // For newest, start from near the end to read the latest messages
                                // Read enough messages to ensure we get the newest ones after sorting
                                var estimatedMessagesToRead = Math.Min(totalLimit * 5, watermarks.High - watermarks.Low);
                                startOffset = Math.Max(watermarks.Low, watermarks.High - estimatedMessagesToRead);
                            }
                            else // "oldest"
                            {
                                // Start from the beginning
                                startOffset = watermarks.Low;
                            }
                        }
                        
                        // Ensure offsets are within valid range
                        startOffset = Math.Max(watermarks.Low, Math.Min(startOffset, watermarks.High));
                        endOffset = Math.Max(watermarks.Low, Math.Min(endOffset, watermarks.High));
                        
                        partitionStartOffsets[partition.Partition] = startOffset;
                        partitionEndOffsets[partition.Partition] = endOffset;
                        
                        tempConsumer.Unassign();
                        tempConsumer.Close();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error getting offsets for partition {partition.Partition}: {ex.Message}");
                        // Use default offsets if we can't get them
                        partitionOffsets[partition.Partition] = new WatermarkOffsets(0, 0);
                        partitionStartOffsets[partition.Partition] = 0;
                        partitionEndOffsets[partition.Partition] = 0;
                    }
                }

                // Create a separate consumer for each partition to avoid state issues
                foreach (var partition in partitions)
                {
                    IConsumer<string, string>? partitionConsumer = null;
                    try
                    {
                        // Create unique consumer config for each partition
                        var partitionConsumerConfig = CreateConsumerConfig(broker, $"reader-{partition.Partition}-{Guid.NewGuid()}");
                        partitionConsumer = new ConsumerBuilder<string, string>(partitionConsumerConfig)
                            .SetKeyDeserializer(Deserializers.Utf8)
                            .SetValueDeserializer(Deserializers.Utf8)
                            .Build();

                        var watermarkOffsets = partitionOffsets[partition.Partition];
                        var startOffset = partitionStartOffsets[partition.Partition];
                        var endOffset = partitionEndOffsets[partition.Partition];

                        // Seek to start offset
                        partitionConsumer.Assign(partition);
                        Thread.Sleep(100);
                        partitionConsumer.Seek(new TopicPartitionOffset(partition, startOffset));

                        // Read messages from this partition
                        // Collect all messages first, then we'll filter and sort
                        var partitionMessages = new List<TopicMessageInfo>();
                        var onlyNewCutoff = DateTime.UtcNow.AddMinutes(-5);

                        // For "newest" or "onlyNew", we need to read from the end backwards
                        // Since Kafka consumer reads forward, we'll read more messages and take the newest ones
                        // If there's a text filter, we can read less since we're filtering
                        var hasTextFilter = !string.IsNullOrWhiteSpace(filter.Query);
                        var maxMessagesToRead = (filter.SearchType == "newest" || filter.SearchType == "onlyNew") 
                            ? (hasTextFilter ? totalLimit * 2 : totalLimit * 5)  // Read less if filtering
                            : (hasTextFilter ? totalLimit * 2 : totalLimit);

                        while (partitionMessages.Count < maxMessagesToRead && messages.Count < totalLimit * 2)
                        {
                            try
                            {
                                var result = partitionConsumer.Consume(readTimeout);
                                if (result == null)
                                {
                                    break; // No more messages
                                }

                                // Check if we've passed the end offset
                                if (result.Offset >= endOffset)
                                {
                                    break;
                                }

                                var messageTimestamp = result.Message.Timestamp.UtcDateTime;

                                // Apply time filter if specified
                                if (hasTimeFilter)
                                {
                                    if (messageTimestamp < startTime || messageTimestamp > endTime)
                                    {
                                        // For oldest search, if we're past end time, stop
                                        if (filter.SearchType == "oldest" && messageTimestamp > endTime)
                                        {
                                            break;
                                        }
                                        // Skip messages outside time range
                                        continue;
                                    }
                                }

                                // Check "only new" filter - only messages from last 5 minutes
                                if (filter.SearchType == "onlyNew")
                                {
                                    if (messageTimestamp < onlyNewCutoff)
                                    {
                                        // For onlyNew, if we hit old messages, stop (we're reading forward from near the end)
                                        break;
                                    }
                                }

                                // Process key: null -> "<Null>", empty string -> "<Empty>"
                                string key;
                                if (result.Message.Key == null)
                                {
                                    key = "{Null}";
                                }
                                else if (string.IsNullOrEmpty(result.Message.Key))
                                {
                                    key = "{Empty}";
                                }
                                else
                                {
                                    key = result.Message.Key;
                                }
                                var value = result.Message.Value ?? string.Empty;

                                // Apply text query filter - only check message value, not key
                                if (!string.IsNullOrWhiteSpace(filter.Query))
                                {
                                    var term = filter.Query.Trim().ToLowerInvariant();
                                    if (!value.ToLowerInvariant().Contains(term))
                                    {
                                        continue; // Doesn't match query - skip this message
                                    }
                                }

                                partitionMessages.Add(new TopicMessageInfo(
                                    filter.TopicName,
                                    result.Partition,
                                    result.Offset,
                                    key,
                                    value,
                                    messageTimestamp));
                                
                                // Early exit optimization: if we have text filter and collected enough matching messages,
                                // we can stop reading from this partition early
                                if (!string.IsNullOrWhiteSpace(filter.Query) && partitionMessages.Count >= totalLimit)
                                {
                                    break;
                                }

                                // Check if we've reached the end of partition
                                if (result.Offset >= watermarkOffsets.High - 1)
                                {
                                    break;
                                }
                            }
                            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_PartitionEOF)
                            {
                                // End of partition
                                break;
                            }
                        }

                        messages.AddRange(partitionMessages);
                    }
                    catch (Exception ex)
                    {
                        // Log error but continue with other partitions
                        Console.WriteLine($"Error reading from partition {partition.Partition}: {ex.Message}");
                    }
                    finally
                    {
                        // Ensure consumer is properly closed
                        try
                        {
                            partitionConsumer?.Unassign();
                            partitionConsumer?.Close();
                            partitionConsumer?.Dispose();
                        }
                        catch
                        {
                            // Ignore errors during cleanup
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading messages from broker {broker.ConnectionName} for topic {filter.TopicName}: {ex.Message}");
                throw;
            }

            // Sort messages
            messages = filter.SearchType switch
            {
                "oldest" => messages.OrderBy(m => m.TimestampUtc).ThenBy(m => m.Partition).ThenBy(m => m.Offset).ToList(),
                _ => messages.OrderByDescending(m => m.TimestampUtc).ThenBy(m => m.Partition).ThenBy(m => m.Offset).ToList()
            };

            // Apply final limit
            if (filter.Limit.HasValue && filter.Limit.Value > 0)
            {
                messages = messages.Take(filter.Limit.Value).ToList();
            }

            return messages;
        }
        public void SendMessage(TopicInfo topic, TopicMessageInfo message)
        {
            // Mock implementation - in real scenario, this would send message to Kafka broker
            // For now, we just log or simulate the operation
            // In production, you would use a Kafka producer library here
            Console.WriteLine($"Sending message to topic '{topic.Name}': Key={message.Key}, Value={message.Value}");
        }
    }
}
