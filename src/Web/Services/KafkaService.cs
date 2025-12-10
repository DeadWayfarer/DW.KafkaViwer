using DW.KafkaViwer.Web.Models;

namespace DW.KafkaViwer.Web.Services;

public class KafkaService
{
    public List<TopicInfo> GetTopics() =>
        new()
        {
            new TopicInfo("payments", 12, 152_340, 7),
            new TopicInfo("notifications", 8, 83_412, 3),
            new TopicInfo("orders", 6, 45_012, 14),
            new TopicInfo("user-updates", 4, 32_001, 10),
            new TopicInfo("audit-log", 3, 9_512, 30)
        };
}

