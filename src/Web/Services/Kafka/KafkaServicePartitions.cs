using DW.KafkaViwer.Web.Models;
using Confluent.Kafka;

namespace DW.KafkaViwer.Web.Services.Kafka;

public partial class KafkaService
{
    /// <summary>
    /// Получает информацию о партициях топика: номер партиции, минимальный и максимальный offset.
    /// </summary>
    public TopicPartitionsInfo GetTopicPartitions(string topicName, int brokerId)
    {
        var brokers = GetBrokers();
        if (!brokers.TryGetValue(brokerId, out var broker))
        {
            throw new ArgumentException($"Broker with ID {brokerId} not found");
        }

        if (broker.Status != "Active")
        {
            throw new InvalidOperationException($"Broker {broker.ConnectionName} is not active");
        }

        try
        {
            var adminConfig = CreateAdminClientConfig(broker);
            using var adminClient = new AdminClientBuilder(adminConfig).Build();
            
            // Get topic metadata
            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);
            
            if (topicMetadata == null)
            {
                throw new ArgumentException($"Topic '{topicName}' not found in broker {broker.ConnectionName}");
            }

            var partitions = new List<PartitionInfo>();
            long totalMessages = 0;

            // Get watermark offsets for each partition
            foreach (var partitionMetadata in topicMetadata.Partitions)
            {
                try
                {
                    var partition = new TopicPartition(topicName, partitionMetadata.PartitionId);
                    var consumerConfig = CreateConsumerConfig(broker, $"partition-info-{Guid.NewGuid()}");
                    
                    using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();
                    consumer.Assign(partition);
                    Thread.Sleep(100);
                    
                    var watermarks = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10));
                    var minOffset = watermarks.Low;
                    var maxOffset = watermarks.High;
                    var partitionMessages = maxOffset - minOffset;
                    
                    partitions.Add(new PartitionInfo(partitionMetadata.PartitionId, minOffset, maxOffset));
                    totalMessages += partitionMessages;
                    
                    consumer.Unassign();
                    consumer.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error getting offsets for partition {partitionMetadata.PartitionId}: {ex.Message}");
                    // Add partition with zero offsets on error
                    partitions.Add(new PartitionInfo(partitionMetadata.PartitionId, 0, 0));
                }
            }

            return new TopicPartitionsInfo(topicName, brokerId, totalMessages, partitions);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error loading partition info for topic {topicName} from broker {broker.ConnectionName}: {ex.Message}");
            throw;
        }
    }
}

