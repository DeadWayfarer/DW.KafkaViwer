using DW.KafkaViwer.Web.Models;
using DW.KafkaViwer.Web.Components;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace DW.KafkaViwer.Web.Services.Kafka;

public partial class KafkaService
{
    private readonly BrokersCache _brokersCache;
    private readonly TopicCache _topicCache;
    private readonly ConsumerCache _consumerCache;

    public KafkaService(
        BrokersCache brokersCache,
        TopicCache topicCache,
        ConsumerCache consumerCache)
    {
        _brokersCache = brokersCache;
        _topicCache = topicCache;
        _consumerCache = consumerCache;
    }

    /// <summary>
    /// Creates a base AdminClientConfig for the specified broker with authentication configured.
    /// </summary>
    protected AdminClientConfig CreateAdminClientConfig(BrokerInfo broker)
    {
        var bootstrapServers = $"{broker.Host}:{broker.Port}";
        
        var config = new AdminClientConfig
        {
            BootstrapServers = bootstrapServers,
            SocketTimeoutMs = 10000
        };

        // Use OAuthBearer if OIDCEndpoint is provided, otherwise use Plain
        if (!string.IsNullOrWhiteSpace(broker.OIDCEndpoint))
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
            config.SaslMechanism = SaslMechanism.OAuthBearer;
            config.SaslOauthbearerTokenEndpointUrl = broker.OIDCEndpoint;
            config.SaslOauthbearerClientId = broker.ClientId;
            config.SaslOauthbearerClientSecret = broker.ClientSecret;
        }

        return config;
    }

    /// <summary>
    /// Creates a base ConsumerConfig for the specified broker with authentication configured.
    /// </summary>
    protected ConsumerConfig CreateConsumerConfig(BrokerInfo broker, string? groupId = null)
    {
        var bootstrapServers = $"{broker.Host}:{broker.Port}";
        
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId ?? $"consumer-{Guid.NewGuid()}",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            SessionTimeoutMs = 10000,
            SocketTimeoutMs = 10000
        };

        // Use OAuthBearer if OIDCEndpoint is provided, otherwise use Plain
        if (!string.IsNullOrWhiteSpace(broker.OIDCEndpoint))
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
            config.SaslMechanism = SaslMechanism.OAuthBearer;
            config.SaslOauthbearerTokenEndpointUrl = broker.OIDCEndpoint;
            config.SaslOauthbearerClientId = broker.ClientId;
            config.SaslOauthbearerClientSecret = broker.ClientSecret;
        }

        return config;
    }

    /// <summary>
    /// Creates a base ProducerConfig for the specified broker with authentication configured.
    /// </summary>
    protected ProducerConfig CreateProducerConfig(BrokerInfo broker)
    {
        var bootstrapServers = $"{broker.Host}:{broker.Port}";
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
        };

        // Use OAuthBearer if OIDCEndpoint is provided, otherwise use Plain
        if (!string.IsNullOrWhiteSpace(broker.OIDCEndpoint))
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
            config.SaslMechanism = SaslMechanism.OAuthBearer;
            config.SaslOauthbearerTokenEndpointUrl = broker.OIDCEndpoint;
            config.SaslOauthbearerClientId = broker.ClientId;
            config.SaslOauthbearerClientSecret = broker.ClientSecret;
        }

        return config;
    }

    public List<ConsumerInfo> GetConsumerInfo(ConsumerFilter filter)
    {
        var consumers = new List<ConsumerInfo>();

        // Iterate through all active brokers to find consumer groups
        foreach (var broker in GetBrokers().Values)
        {
            if (broker.Status != "Active")
            {
                continue;
            }

            try
            {
                var brokerConsumers = LoadConsumersFromBroker(broker, filter.TopicName);
                consumers.AddRange(brokerConsumers);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading consumers from broker {broker.ConnectionName} ({broker.Host}:{broker.Port}): {ex.Message}");
            }
        }

        return consumers;
    }

}
