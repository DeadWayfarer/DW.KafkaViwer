using Confluent.Kafka.Admin;
using Confluent.Kafka;
using DW.KafkaViwer.Web.Models;

namespace DW.KafkaViwer.Web.Services.Kafka
{
    public partial class KafkaService
    {
        public List<TopicInfo> GetTopics(TopicFilter filter)
        {
            var allTopics = new List<TopicInfo>();

            // Load topics from each broker (cluster)
            foreach (var broker in _brokers.Values)
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
                .OrderBy(t => t.Name)
                .ToList();

            return allTopics;
        }

        private List<TopicInfo> LoadTopicsFromBroker(BrokerInfo broker)
        {
            var topics = new List<TopicInfo>();
            var bootstrapServers = $"{broker.Host}:{broker.Port}";

            try
            {
                var config = CreateAdminClientConfig(broker);
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
                            var consumerConfig = CreateConsumerConfig(broker, $"topic-viewer-{Guid.NewGuid()}");
                            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

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
                        broker.Id,
                        broker.ConnectionName));
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
    }
}
