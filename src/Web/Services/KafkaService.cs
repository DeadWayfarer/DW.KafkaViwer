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
}

