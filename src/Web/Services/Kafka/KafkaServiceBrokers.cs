using DW.KafkaViwer.Web.Models;

namespace DW.KafkaViwer.Web.Services.Kafka
{
    public partial class KafkaService
    {
        public List<BrokerInfo> GetBrokers()
        {
            return _brokers .Values.ToList();
        }

        public void UpdateBroker(BrokerInfo brokerInfo)
        {
            _brokers[brokerInfo.Id] = brokerInfo;
        }

        public void AddBroker(BrokerInfo brokerInfo)
        {
            var newId = _brokers.Count > 0 ? _brokers.Keys.Max() + 1 : 1;
            var newBroker = new BrokerInfo(newId, brokerInfo.ConnectionName, brokerInfo.Host, brokerInfo.Port, brokerInfo.Status,
                brokerInfo.ClientId, brokerInfo.ClientSecret, brokerInfo.OIDCEndpoint);
            _brokers.Add(newBroker.Id, newBroker);
        }

        public void DeleteBroker(BrokerInfo brokerInfo)
        {
            _brokers.Remove(brokerInfo.Id);
        }

    }
}
