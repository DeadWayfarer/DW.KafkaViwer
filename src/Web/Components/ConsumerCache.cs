using DW.KafkaViwer.Web.Models;
using System.Collections.Generic;
using System.Linq;

namespace DW.KafkaViwer.Web.Components;

public class ConsumerCache {
    // Key: brokerId, Value: Dictionary<groupId, ConsumerInfo>
    private readonly Dictionary<int, Dictionary<string, ConsumerInfo>> _consumersByBroker = new();

    public void AddConsumers(List<ConsumerInfo> consumers)
    {
        foreach (var consumer in consumers)
        {
            if (!_consumersByBroker.ContainsKey(consumer.BrokerId))
            {
                _consumersByBroker[consumer.BrokerId] = new Dictionary<string, ConsumerInfo>();
            }
            _consumersByBroker[consumer.BrokerId][consumer.Group] = consumer;
        }
    }

    public IReadOnlyList<ConsumerInfo> GetConsumers(string? topicName = null)
    {
        var allConsumers = new List<ConsumerInfo>();
        
        foreach (var brokerConsumers in _consumersByBroker.Values)
        {
            foreach (var consumer in brokerConsumers.Values)
            {
                if (topicName == null)
                {
                    // Return all consumers
                    allConsumers.Add(consumer);
                }
                else
                {
                    // Filter by topic - check if consumer has partitions for this topic
                    if (consumer.Partitions != null && consumer.Partitions.Any(p => p.Topic == topicName))
                    {
                        // Create filtered consumer info with only relevant partitions
                        var filteredPartitions = consumer.Partitions.Where(p => p.Topic == topicName).ToList();
                        var filteredMembers = consumer.Members?.Select(m => 
                        {
                            var memberPartitions = m.Partitions?.Where(p => p.Topic == topicName).ToList() ?? new List<PartitionLagInfo>();
                            return new MemberDetailInfo(m.MemberId, m.ClientId, m.Host, memberPartitions);
                        }).Where(m => m.Partitions.Count > 0).ToList();

                        var totalLag = filteredPartitions.Sum(p => p.Lag);
                        
                        var filteredConsumer = new ConsumerInfo(
                            consumer.Group,
                            consumer.Member,
                            (int)totalLag,
                            consumer.Status,
                            consumer.BrokerId,
                            consumer.BrokerName,
                            filteredMembers?.Count > 0 ? filteredMembers : null,
                            filteredPartitions.Count > 0 ? filteredPartitions : null);
                        
                        allConsumers.Add(filteredConsumer);
                    }
                }
            }
        }
        
        return allConsumers;
    }

    public void Clear()
    {
        _consumersByBroker.Clear();
    }
}