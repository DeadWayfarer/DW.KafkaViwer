using DW.KafkaViwer.Web.Models;
using System.Collections.Generic;
using System.Linq;

namespace DW.KafkaViwer.Web.Components;

public class TopicCache
{
    // Key: $"{BrokerId}:{TopicName}" to avoid collisions across brokers
    private readonly Dictionary<string, TopicInfo> _topics = new();

    public void AddTopics(List<TopicInfo> topics)
    {
        foreach (var topic in topics)
        {
            var key = $"{topic.BrokerId}:{topic.Name}";
            _topics[key] = topic;
        }
    }

    public IReadOnlyList<TopicInfo> GetTopics()
    {
        return _topics.Values.ToList();
    }
}