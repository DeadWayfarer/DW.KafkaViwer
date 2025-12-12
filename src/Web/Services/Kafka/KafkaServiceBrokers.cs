using DW.KafkaViwer.Web.Models;

namespace DW.KafkaViwer.Web.Services.Kafka
{
    public partial class KafkaService
    {
        public IReadOnlyDictionary<int, BrokerInfo> GetBrokers()
        {
            return _brokersCache.GetBrokers();
        }

        public void UpdateBroker(BrokerInfo brokerInfo)
        {
            _brokersCache.UpdateBroker(brokerInfo);
        }

        public void AddBroker(BrokerInfo brokerInfo)
        {
            _brokersCache.AddBroker(brokerInfo);
        }

        public void DeleteBroker(BrokerInfo brokerInfo)
        {
            _brokersCache.DeleteBroker(brokerInfo);
        }

    }
}
