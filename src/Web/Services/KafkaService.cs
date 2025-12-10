using DW.KafkaViwer.Web.Models;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace DW.KafkaViwer.Web.Services;

public class KafkaService
{
    private readonly List<BrokerInfo> _brokers;

    public KafkaService(List<BrokerInfo> brokers)
    {
        _brokers = brokers ?? new List<BrokerInfo>();
    }

    public List<TopicInfo> GetTopics(TopicFilter filter)
    {
        var allTopics = new List<TopicInfo>();

        // Load topics from each broker (cluster)
        foreach (var broker in _brokers)
        {
            // Skip inactive brokers
            if (broker.Status != "Active")
            {
                continue;
            }

            try
            {
                // Load topics from this broker/cluster
                var brokerTopics = LoadTopicsFromBroker(broker);
                allTopics.AddRange(brokerTopics);
            }
            catch (Exception ex)
            {
                // Log error but continue with other brokers
                Console.WriteLine($"Error loading topics from broker {broker.ConnectionName} ({broker.Host}:{broker.Port}): {ex.Message}");
            }
        }

        // Apply name filter if provided
        if (!string.IsNullOrWhiteSpace(filter.Name))
        {
            var term = filter.Name.Trim().ToLowerInvariant();
            allTopics = allTopics
                .Where(t => t.Name.ToLowerInvariant().Contains(term))
                .ToList();
        }

        // Remove duplicates (same topic name from different clusters)
        // In real scenario, you might want to keep them separate or merge metadata
        allTopics = allTopics
            .GroupBy(t => t.Name)
            .Select(g => g.First())
            .ToList();

        return allTopics;
    }

    private List<TopicInfo> LoadTopicsFromBroker(BrokerInfo broker)
    {
        var topics = new List<TopicInfo>();
        var bootstrapServers = $"{broker.Host}:{broker.Port}";

        var config = new AdminClientConfig
        {
            BootstrapServers = bootstrapServers,
            SocketTimeoutMs = 10000
        };

        // Configure authentication if provided
        if (!string.IsNullOrWhiteSpace(broker.ClientId) && !string.IsNullOrWhiteSpace(broker.ClientSecret))
        {
            config.SaslMechanism = SaslMechanism.Plain;
            config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
            config.SaslUsername = broker.ClientId;
            config.SaslPassword = broker.ClientSecret;
        }

        try
        {
            using var adminClient = new AdminClientBuilder(config).Build();

            // Get metadata to retrieve topics
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

            foreach (var topicMetadata in metadata.Topics)
            {
                // Skip internal topics (starting with __)
                if (topicMetadata.Topic.StartsWith("__"))
                {
                    continue;
                }

                // Get partition count
                var partitionCount = topicMetadata.Partitions.Count;

                // Try to get topic configuration for retention days
                int retentionDays = 7; // Default value
                try
                {
                    var topicConfigResource = new ConfigResource
                    {
                        Type = ResourceType.Topic,
                        Name = topicMetadata.Topic
                    };

                    var describeResult = adminClient.DescribeConfigsAsync(
                        new[] { topicConfigResource },
                        new DescribeConfigsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                    ).Result;

                    if (describeResult.Count > 0 && describeResult[0].Entries.TryGetValue("retention.ms", out var retentionMsConfig))
                    {
                        if (long.TryParse(retentionMsConfig.Value, out var retentionMs))
                        {
                            retentionDays = (int)(retentionMs / (1000L * 60 * 60 * 24));
                        }
                    }
                }
                catch
                {
                    // If we can't get retention config, use default
                }

                // Get message count (approximate) - this is a simplified approach
                // In production, you might want to use a consumer to get exact counts
                long messageCount = 0;
                try
                {
                    foreach (var partition in topicMetadata.Partitions)
                    {
                        // Get high watermark (last offset) for approximate message count
                        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
                        {
                            BootstrapServers = bootstrapServers,
                            GroupId = $"topic-viewer-{Guid.NewGuid()}",
                            EnableAutoCommit = false,
                            AutoOffsetReset = AutoOffsetReset.Earliest
                        }).Build();

                        var topicPartition = new TopicPartition(topicMetadata.Topic, partition.PartitionId);
                        var watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(10));
                        messageCount += watermarkOffsets.High - watermarkOffsets.Low;

                        consumer.Close();
                    }
                }
                catch
                {
                    // If we can't get message count, use 0
                    messageCount = 0;
                }

                topics.Add(new TopicInfo(
                    topicMetadata.Topic,
                    partitionCount,
                    messageCount,
                    retentionDays,
                    broker.Id));
            }
        }
        catch (Exception ex)
        {
            // Log error and rethrow to be handled by caller
            Console.WriteLine($"Error loading topics from broker {broker.ConnectionName} ({bootstrapServers}): {ex.Message}");
            throw;
        }

        return topics;
    }

    public List<TopicMessageInfo> GetTopicMessages(TopicMessageFilter filter)
    {
        // Find broker by ID
        var broker = _brokers.FirstOrDefault(b => b.Id == filter.BrokerId);
        if (broker == null)
        {
            throw new ArgumentException($"Broker with ID {filter.BrokerId} not found");
        }

        if (broker.Status != "Active")
        {
            throw new InvalidOperationException($"Broker {broker.ConnectionName} is not active");
        }

        var messages = new List<TopicMessageInfo>();
        var bootstrapServers = $"{broker.Host}:{broker.Port}";

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"topic-viewer-{Guid.NewGuid()}",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            SessionTimeoutMs = 10000,
            SocketTimeoutMs = 10000
        };

        // Configure authentication if provided
        if (!string.IsNullOrWhiteSpace(broker.ClientId) && !string.IsNullOrWhiteSpace(broker.ClientSecret))
        {
            consumerConfig.SaslMechanism = SaslMechanism.Plain;
            consumerConfig.SecurityProtocol = SecurityProtocol.SaslPlaintext;
            consumerConfig.SaslUsername = broker.ClientId;
            consumerConfig.SaslPassword = broker.ClientSecret;
        }

        try
        {
            // Get metadata using AdminClient
            var adminConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                SocketTimeoutMs = 10000
            };

            if (!string.IsNullOrWhiteSpace(broker.ClientId) && !string.IsNullOrWhiteSpace(broker.ClientSecret))
            {
                adminConfig.SaslMechanism = SaslMechanism.Plain;
                adminConfig.SecurityProtocol = SecurityProtocol.SaslPlaintext;
                adminConfig.SaslUsername = broker.ClientId;
                adminConfig.SaslPassword = broker.ClientSecret;
            }

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
                    var tempConfig = new ConsumerConfig
                    {
                        BootstrapServers = bootstrapServers,
                        GroupId = $"watermark-{Guid.NewGuid()}",
                        EnableAutoCommit = false,
                        AutoOffsetReset = AutoOffsetReset.Earliest,
                        SessionTimeoutMs = 10000,
                        SocketTimeoutMs = 10000
                    };

                    if (!string.IsNullOrWhiteSpace(broker.ClientId) && !string.IsNullOrWhiteSpace(broker.ClientSecret))
                    {
                        tempConfig.SaslMechanism = SaslMechanism.Plain;
                        tempConfig.SecurityProtocol = SecurityProtocol.SaslPlaintext;
                        tempConfig.SaslUsername = broker.ClientId;
                        tempConfig.SaslPassword = broker.ClientSecret;
                    }

                    using var tempConsumer = new ConsumerBuilder<Ignore, Ignore>(tempConfig).Build();
                    tempConsumer.Assign(partition);
                    // Small delay to ensure assignment is complete
                    System.Threading.Thread.Sleep(100);
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
                    var partitionConsumerConfig = new ConsumerConfig
                    {
                        BootstrapServers = bootstrapServers,
                        GroupId = $"reader-{partition.Partition}-{Guid.NewGuid()}",
                        EnableAutoCommit = false,
                        AutoOffsetReset = AutoOffsetReset.Earliest,
                        SessionTimeoutMs = 10000,
                        SocketTimeoutMs = 10000
                    };

                    if (!string.IsNullOrWhiteSpace(broker.ClientId) && !string.IsNullOrWhiteSpace(broker.ClientSecret))
                    {
                        partitionConsumerConfig.SaslMechanism = SaslMechanism.Plain;
                        partitionConsumerConfig.SecurityProtocol = SecurityProtocol.SaslPlaintext;
                        partitionConsumerConfig.SaslUsername = broker.ClientId;
                        partitionConsumerConfig.SaslPassword = broker.ClientSecret;
                    }

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

    public List<ConsumerInfo> GetConsumerInfo(ConsumerFilter filter)
    {
        // Mock consumer data
        return new List<ConsumerInfo>
        {
            new(filter.TopicName + "-grp", "consumer-1", 12, "Active"),
            new(filter.TopicName + "-grp", "consumer-2", 3, "Active"),
            new(filter.TopicName + "-grp", "consumer-3", 25, "Rebalancing")
        };
    }

    public List<BrokerInfo> GetBrokers()
    {
        return _brokers.ToList();
    }

    public void UpdateBroker(BrokerInfo brokerInfo)
    {
        var index = _brokers.FindIndex(b => b.Id == brokerInfo.Id);
        if (index >= 0)
        {
            _brokers[index] = brokerInfo;
        }
    }

    public void AddBroker(BrokerInfo brokerInfo)
    {
        var newId = _brokers.Count > 0 ? _brokers.Max(b => b.Id) + 1 : 1;
        var newBroker = new BrokerInfo(newId, brokerInfo.ConnectionName, brokerInfo.Host, brokerInfo.Port, brokerInfo.Status, 
            brokerInfo.ClientId, brokerInfo.ClientSecret, brokerInfo.OIDCEndpoint);
        _brokers.Add(newBroker);
    }

    public void DeleteBroker(BrokerInfo brokerInfo)
    {
        var index = _brokers.FindIndex(b => b.Id == brokerInfo.Id);
        if (index >= 0)
        {
            _brokers.RemoveAt(index);
        }
    }

    public void SendMessage(TopicInfo topic, TopicMessageInfo message)
    {
        // Mock implementation - in real scenario, this would send message to Kafka broker
        // For now, we just log or simulate the operation
        // In production, you would use a Kafka producer library here
        Console.WriteLine($"Sending message to topic '{topic.Name}': Key={message.Key}, Value={message.Value}");
    }
}

