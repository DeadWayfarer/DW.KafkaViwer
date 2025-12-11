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

                // Determine offset strategy based on SearchType
                var endTime = filter.To ?? DateTime.UtcNow;
                var startTime = filter.From ?? DateTime.MinValue;
                var readTimeout = TimeSpan.FromSeconds(5);
                var maxMessagesPerPartition = (filter.Limit ?? 1000) / Math.Max(partitions.Count, 1); // Distribute limit across partitions

                // Get watermark offsets - need to assign partition first
                var partitionOffsets = new Dictionary<int, WatermarkOffsets>();
                foreach (var partition in partitions)
                {
                    try
                    {
                        var tempConfig = CreateConsumerConfig(broker, $"watermark-{Guid.NewGuid()}");
                        using var tempConsumer = new ConsumerBuilder<Ignore, Ignore>(tempConfig).Build();
                        tempConsumer.Assign(partition);
                        // Small delay to ensure assignment is complete
                        Thread.Sleep(100);
                        partitionOffsets[partition.Partition] = tempConsumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10));
                        tempConsumer.Unassign();
                        tempConsumer.Close();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error getting watermark for partition {partition.Partition}: {ex.Message}");
                        // Use default offsets if we can't get them
                        partitionOffsets[partition.Partition] = new WatermarkOffsets(0, 0);
                    }
                }

                // Create a separate consumer for each partition to avoid state issues
                foreach (var partition in partitions)
                {
                    IConsumer<Ignore, string>? partitionConsumer = null;
                    try
                    {
                        // Create unique consumer config for each partition
                        var partitionConsumerConfig = CreateConsumerConfig(broker, $"reader-{partition.Partition}-{Guid.NewGuid()}");
                        partitionConsumer = new ConsumerBuilder<Ignore, string>(partitionConsumerConfig)
                            .SetValueDeserializer(Deserializers.Utf8)
                            .Build();

                        var watermarkOffsets = partitionOffsets[partition.Partition];

                        // Determine starting offset
                        long startOffset = filter.SearchType switch
                        {
                            "oldest" => watermarkOffsets.Low,
                            "onlyNew" => Math.Max(watermarkOffsets.Low, watermarkOffsets.High - 100), // Last 100 messages
                            _ => watermarkOffsets.High - Math.Min(watermarkOffsets.High - watermarkOffsets.Low, 1000) // Last 1000 or all if less
                        };

                        // Seek to start offset
                        partitionConsumer.Assign(partition);
                        Thread.Sleep(100);
                        partitionConsumer.Seek(new TopicPartitionOffset(partition, startOffset));


                        // Read messages from this partition
                        var partitionMessages = new List<TopicMessageInfo>();

                        while (partitionMessages.Count < maxMessagesPerPartition && messages.Count < (filter.Limit ?? 1000))
                        {
                            try
                            {
                                var result = partitionConsumer.Consume(readTimeout);
                                if (result == null)
                                {
                                    break; // No more messages
                                }

                                var messageTimestamp = result.Message.Timestamp.UtcDateTime;

                                // Check time window
                                if (messageTimestamp < startTime || messageTimestamp > endTime)
                                {
                                    if (filter.SearchType == "oldest" && messageTimestamp > endTime)
                                    {
                                        break; // Past the end time
                                    }
                                    if (filter.SearchType != "oldest" && messageTimestamp < startTime)
                                    {
                                        continue; // Before start time, but continue reading
                                    }
                                }

                                // Check "only new" filter
                                if (filter.SearchType == "onlyNew")
                                {
                                    var cutoff = DateTime.UtcNow.AddMinutes(-5);
                                    if (messageTimestamp < cutoff)
                                    {
                                        break; // Too old for "only new"
                                    }
                                }

                                var key = result.Message.Key?.ToString() ?? string.Empty;
                                var value = result.Message.Value ?? string.Empty;

                                // Apply text query filter
                                if (!string.IsNullOrWhiteSpace(filter.Query))
                                {
                                    var term = filter.Query.Trim().ToLowerInvariant();
                                    if (!key.ToLowerInvariant().Contains(term) && !value.ToLowerInvariant().Contains(term))
                                    {
                                        continue; // Doesn't match query
                                    }
                                }

                                partitionMessages.Add(new TopicMessageInfo(
                                    filter.TopicName,
                                    result.Partition,
                                    result.Offset,
                                    key,
                                    value,
                                    messageTimestamp));

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
