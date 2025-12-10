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
        var rnd = new Random(42);
        var messages = new List<TopicMessageInfo>();
        for (var i = 0; i < 25; i++)
        {
            messages.Add(new TopicMessageInfo(
                filter.TopicName,
                Partition: rnd.Next(0, 3),
                Offset: 1_000 + i,
                Key: $"key-{i}",
                Value: $"Message payload #{i} for {filter.TopicName}",
                TimestampUtc: DateTime.UtcNow.AddSeconds(-rnd.Next(0, 10_000))));
        }

        // Apply "only new" as last 5 minutes for mock
        if (filter.SearchType == "onlyNew")
        {
            var cutoff = DateTime.UtcNow.AddMinutes(-5);
            messages = messages.Where(m => m.TimestampUtc >= cutoff).ToList();
        }

        // Apply time window
        if (filter.From.HasValue)
        {
            messages = messages.Where(m => m.TimestampUtc >= filter.From.Value).ToList();
        }
        if (filter.To.HasValue)
        {
            messages = messages.Where(m => m.TimestampUtc <= filter.To.Value).ToList();
        }

        // Apply text query to key/value
        if (!string.IsNullOrWhiteSpace(filter.Query))
        {
            var term = filter.Query.Trim().ToLowerInvariant();
            messages = messages.Where(m =>
                m.Key.ToLowerInvariant().Contains(term) ||
                m.Value.ToLowerInvariant().Contains(term)).ToList();
        }

        // Sort
        messages = filter.SearchType switch
        {
            "oldest" => messages.OrderBy(m => m.TimestampUtc).ToList(),
            _ => messages.OrderByDescending(m => m.TimestampUtc).ToList()
        };

        // Limit
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

