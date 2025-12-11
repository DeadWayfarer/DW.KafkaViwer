using DW.KafkaViwer.Web.Models;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace DW.KafkaViwer.Web.Services.Kafka;

public partial class KafkaService
{
    private readonly Dictionary<int, BrokerInfo> _brokers;

    public KafkaService(Dictionary<int, BrokerInfo> brokers)
    {
        _brokers = brokers ?? new Dictionary<int, BrokerInfo>();
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
}

