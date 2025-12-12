using DW.KafkaViwer.Web.Models;

namespace DW.KafkaViwer.Web.Components;

public class TopicCache
{
    private readonly Dictionary<string, TopicInfo> _topics = new Dictionary<string, TopicInfo>();

    public void AddTopics(List<TopicInfo> topics)
    {
        foreach (var topic in topics)
        {
            _topics[topic.Name] = topic;
        }
    }

    public IReadOnlyList<TopicInfo> GetTopics()
    {
        return _topics.Values.ToList().AsReadOnly();
    }
}