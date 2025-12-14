using System.Threading.Tasks;
using DW.KafkaViwer.Web.Components;
using DW.KafkaViwer.Web.Models;
using Confluent.Kafka.Admin;
using Confluent.Kafka;

namespace DW.KafkaViwer.Web.Services.Kafka;

/// <summary>
/// Background helper for Kafka-related preloading tasks.
/// Currently contains placeholder methods.
/// </summary>
public partial class KafkaService
{
    public void LoadTopics()
    {
        var allTopics = new List<TopicInfo>();
        foreach (var broker in _brokersCache.GetBrokers().Values)
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

        // Remove duplicates (same topic name from different clusters)
        // In real scenario, you might want to keep them separate or merge metadata
        allTopics = allTopics
            .GroupBy(t => t.Name)
            .Select(g => g.First())
            .OrderBy(t => t.Name)
            .ToList();

        _topicCache.AddTopics(allTopics);
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

                    topics.Add(new TopicInfo(
                        topicMetadata.Topic,
                        partitionCount,
                        null,
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
    
    /// <summary>
    /// Загружает количество сообщений для каждого топика и обновляет TopicCache.
    /// </summary>
    public void LoadTopicsMessageCounts()
    {
        // Убедимся, что топики загружены
        LoadTopics();
        var currentTopics = _topicCache.GetTopics().ToList();
        var updatedTopics = new List<TopicInfo>();

        foreach (var topic in currentTopics)
        {
            LoadTopicMessageCount(topic);
        }

        // Перезаписать cache обновленными топиками
        _topicCache.AddTopics(updatedTopics);
    }

    public void LoadTopicMessageCount(string topicName)
    {
        var topic = _topicCache.GetTopics().FirstOrDefault(t => t.Name == topicName);
        if (topic == null)
            return;

        LoadTopicMessageCount(topic);
    }

    public void LoadTopicMessageCount(TopicInfo topic)
    {
        try
        {
            // Найти брокер, которому принадлежит топик (по BrokerId)
            var brokers = _brokersCache.GetBrokers();
            if (!brokers.TryGetValue(topic.BrokerId, out var broker))
                return;

            var adminConfig = CreateAdminClientConfig(broker);
            using var adminClient = new Confluent.Kafka.AdminClientBuilder(adminConfig).Build();

            // Получить метаданные по топику
            var metadata = adminClient.GetMetadata(topic.Name, TimeSpan.FromSeconds(10));
            long messages = 0;

            // Для всех партиций топика получаем offsetLast - offsetFirst
            foreach (var partition in metadata.Topics.SelectMany(t => t.Partitions))
            {
                var tp = new Confluent.Kafka.TopicPartition(topic.Name, partition.PartitionId);

                // Используем Consumer для QueryWatermarkOffsets (в AdminClient метода нет)
                var consumerConfig = CreateConsumerConfig(broker, $"watermark-{Guid.NewGuid()}");
                using var consumer = new Confluent.Kafka.ConsumerBuilder<Confluent.Kafka.Ignore, Confluent.Kafka.Ignore>(consumerConfig).Build();
                var watermarks = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
                messages += (watermarks.High - watermarks.Low);
                consumer.Close();
            }

            // Создаем новый TopicInfo с обновленным Messages
            var updated = topic with { Messages = messages };
            _topicCache.AddTopics(new List<TopicInfo> { updated });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка при получении количества сообщений для топика {topic.Name}: {ex.Message}");
        }
    }

    /// <summary>
    /// Загружает все группы консьюмеров со всех брокеров и сохраняет в кеш.
    /// </summary>
    public void LoadConsumerGroups()
    {
        var allConsumers = new List<ConsumerInfo>();

        foreach (var broker in _brokersCache.GetBrokers().Values)
        {
            // Skip inactive brokers
            if (broker.Status != "Active")
            {
                continue;
            }

            try
            {
                // Load all consumers from this broker (without topic filter)
                var brokerConsumers = LoadConsumersFromBroker(broker, null);
                allConsumers.AddRange(brokerConsumers);
            }
            catch (Exception ex)
            {
                // Log error but continue with other brokers
                Console.WriteLine($"Error loading consumer groups from broker {broker.ConnectionName} ({broker.Host}:{broker.Port}): {ex.Message}");
            }
        }

        // Update cache with all loaded consumers
        _consumerCache.AddConsumers(allConsumers);
    }
}
