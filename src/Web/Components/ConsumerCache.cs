using DW.KafkaViwer.Web.Models;

namespace DW.KafkaViwer.Web.Components;

public class ConsumerCache {
    private readonly Dictionary<string, ConsumerInfo> _consumers = new Dictionary<string, ConsumerInfo>();

    public void AddConsumers(List<ConsumerInfo> consumers)
    {
        foreach (var consumer in consumers)
        {
            _consumers[consumer.Group] = consumer;
        }
    }

    public IReadOnlyList<ConsumerInfo> GetConsumers()
    {
        return _consumers.Values.ToList().AsReadOnly();
    }
}