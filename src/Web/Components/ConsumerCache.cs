using DW.KafkaViwer.Web.Models;
using System.Collections.Generic;
using System.Linq;

namespace DW.KafkaViwer.Web.Components;

public class ConsumerCache {
    private readonly Dictionary<string, ConsumerInfo> _consumers = new();

    public void AddConsumers(List<ConsumerInfo> consumers)
    {
        foreach (var consumer in consumers)
        {
            _consumers[consumer.Group] = consumer;
        }
    }

    public IReadOnlyList<ConsumerInfo> GetConsumers()
    {
        return _consumers.Values.ToList();
    }
}