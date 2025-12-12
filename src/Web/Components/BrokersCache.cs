using DW.KafkaViwer.Web.Models;

namespace DW.KafkaViwer.Web.Components;

public class BrokersCache {
    private readonly Dictionary<int, BrokerInfo> _brokers = new Dictionary<int, BrokerInfo>();

    public BrokersCache(Dictionary<int, BrokerInfo> brokers)
    {
        _brokers = brokers;
    }

    public IReadOnlyDictionary<int, BrokerInfo> GetBrokers()
    {
        return _brokers;
    }

    public void UpdateBroker(BrokerInfo brokerInfo)
    {
        _brokers[brokerInfo.Id] = brokerInfo;
    }

    public void AddBroker(BrokerInfo brokerInfo)
    {
        _brokers[brokerInfo.Id] = brokerInfo;
    }
    
    public void DeleteBroker(BrokerInfo brokerInfo)
    {
        _brokers.Remove(brokerInfo.Id);
    }
}