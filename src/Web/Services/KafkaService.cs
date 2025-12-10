using DW.KafkaViwer.Web.Models;

namespace DW.KafkaViwer.Web.Services;

public class KafkaService
{
    public List<TopicInfo> GetTopics(TopicFilter filter)
    {
        var data = new List<TopicInfo>
        {
            new("payments", 12, 152_340, 7),
            new("notifications", 8, 83_412, 3),
            new("orders", 6, 45_012, 14),
            new("user-updates", 4, 32_001, 10),
            new("audit-log", 3, 9_512, 30)
        };

        if (!string.IsNullOrWhiteSpace(filter.Name))
        {
            var term = filter.Name.Trim().ToLowerInvariant();
            data = data
                .Where(t => t.Name.ToLowerInvariant().Contains(term))
                .ToList();
        }

        return data;
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

    private static List<BrokerInfo> _brokers = new()
    {
        new BrokerInfo(1, "localhost", 9092, "Active"),
        new BrokerInfo(2, "localhost", 9093, "Active"),
        new BrokerInfo(3, "localhost", 9094, "Inactive")
    };

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
}

