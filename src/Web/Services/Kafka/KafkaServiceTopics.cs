using Confluent.Kafka.Admin;
using Confluent.Kafka;
using DW.KafkaViwer.Web.Models;

namespace DW.KafkaViwer.Web.Services.Kafka
{
    public partial class KafkaService
    {
        public List<TopicInfo> GetTopics(TopicFilter filter)
        {
            var topics = _topicCache.GetTopics();
            if (topics.Count == 0)
            {
                LoadTopics();
                topics = _topicCache.GetTopics();
            }

            // Apply name filter if provided
            if (!string.IsNullOrWhiteSpace(filter.Name))
            {
                var term = filter.Name.Trim().ToLowerInvariant();
                topics = topics
                    .Where(t => t.Name.ToLowerInvariant().Contains(term))
                    .ToList();
            }

            return topics.ToList();
        }
    }
}
