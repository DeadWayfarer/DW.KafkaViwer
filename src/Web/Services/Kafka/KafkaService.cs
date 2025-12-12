using DW.KafkaViwer.Web.Models;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace DW.KafkaViwer.Web.Services.Kafka;

public partial class KafkaService
{
    private readonly Dictionary<int, BrokerInfo> _brokers;

    public KafkaService(Dictionary<int, BrokerInfo> brokers)
    {
        _brokers = brokers ?? new Dictionary<int, BrokerInfo>();
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
        foreach (var broker in _brokers.Values)
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

    private List<ConsumerInfo> LoadConsumersFromBroker(BrokerInfo broker, string topicName)
    {
        var consumers = new List<ConsumerInfo>();

        try
        {
            var adminConfig = CreateAdminClientConfig(broker);
            using var adminClient = new AdminClientBuilder(adminConfig).Build();

            // Try to list consumer groups
            // Note: This method may not be available in all Kafka versions
            // If not available, we'll need to use an alternative approach
            try
            {
                var listResult = adminClient.ListConsumerGroupsAsync(
                    new ListConsumerGroupsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                ).Result;
                
                if (listResult != null && listResult.Valid != null)
                {
                    var groupIds = listResult.Valid.Select(g => g.GroupId).ToList();
                    
                    // Describe consumer groups to get detailed information
                    if (groupIds.Count > 0)
                    {
                        var describeResult = adminClient.DescribeConsumerGroupsAsync(
                            groupIds,
                            new DescribeConsumerGroupsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                        ).Result
                        .ConsumerGroupDescriptions.ToDictionary(x => x.GroupId);

                        if (describeResult != null)
                        {
                            // Iterate through group IDs and try to access descriptions
                            foreach (var groupId in groupIds)
                            {
                                try
                                {
                                    // Try to access the group description using indexer
                                    ConsumerGroupDescription? groupDescription = null;
                                    
                                    // DescribeConsumerGroupsResult might be indexable by groupId
                                    if (describeResult is IReadOnlyDictionary<string, ConsumerGroupDescription> dict)
                                    {
                                        if (!dict.TryGetValue(groupId, out groupDescription) || groupDescription == null)
                                        {
                                            continue;
                                        }
                                    }
                                    else
                                    {
                                        // Fallback: try to iterate if it's enumerable
                                        continue;
                                    }

                                    // Get offsets for this consumer group
                                    var offsetsRequest = new ConsumerGroupTopicPartitions(groupId, null);
                                    
                                    var offsetsResult = adminClient.ListConsumerGroupOffsetsAsync(
                                        new[] { offsetsRequest },
                                        new ListConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                                    ).Result;

                                    if (offsetsResult != null && offsetsResult.Count > 0 && offsetsResult[0].Partitions != null)
                                    {
                                        // Check if this group consumes the specified topic
                                        var topicPartitions = offsetsResult[0].Partitions
                                            .Where(p => p.Topic == topicName)
                                            .ToList();

                                        if (topicPartitions.Count > 0)
                                        {
                                            // Get watermark offsets for lag calculation
                                            var consumerConfig = CreateConsumerConfig(broker, $"lag-calculator-{Guid.NewGuid()}");
                                            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

                                            // Process each member of the consumer group
                                            if (groupDescription.Members != null && groupDescription.Members.Count > 0)
                                            {
                                                foreach (var member in groupDescription.Members)
                                                {
                                                    // Calculate total lag for this member
                                                    int totalLag = 0;
                                                    foreach (var partition in topicPartitions)
                                                    {
                                                        try
                                                        {
                                                            var topicPartition = new TopicPartition(partition.Topic, partition.Partition);
                                                            var watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(10));
                                                            
                                                            // Consumer offset (where the consumer is currently reading)
                                                            long consumerOffset = partition.Offset;
                                                            
                                                            // High watermark (last committed offset in the partition)
                                                            long highWatermark = watermarkOffsets.High;
                                                            
                                                            // Lag is the difference
                                                            long lag = Math.Max(0, highWatermark - consumerOffset);
                                                            totalLag += (int)lag;
                                                        }
                                                        catch
                                                        {
                                                            // If we can't get lag for this partition, skip it
                                                        }
                                                    }

                                                    // Determine status
                                                    string status = groupDescription.State switch
                                                    {
                                                        ConsumerGroupState.Stable => "Active",
                                                        ConsumerGroupState.Dead => "Dead",
                                                        ConsumerGroupState.Empty => "Empty",
                                                        ConsumerGroupState.PreparingRebalance => "Rebalancing",
                                                        ConsumerGroupState.CompletingRebalance => "Rebalancing",
                                                        _ => "Unknown"
                                                    };

                                                    // Get member ID - try different property names
                                                    string memberId = "unknown";
                                                    if (member is MemberDescription memberDesc)
                                                    {
                                                        // Try to get member ID from available properties
                                                        memberId = memberDesc.ClientId ?? memberDesc.Host ?? "unknown";
                                                    }

                                                    consumers.Add(new ConsumerInfo(
                                                        groupDescription.GroupId,
                                                        memberId,
                                                        totalLag,
                                                        status));
                                                }
                                            }
                                            else
                                            {
                                                // No members, but group exists - calculate lag for the group
                                                int totalLag = 0;
                                                foreach (var partition in topicPartitions)
                                                {
                                                    try
                                                    {
                                                        var topicPartition = new TopicPartition(partition.Topic, partition.Partition);
                                                        var watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(10));
                                                        long consumerOffset = partition.Offset;
                                                        long highWatermark = watermarkOffsets.High;
                                                        long lag = Math.Max(0, highWatermark - consumerOffset);
                                                        totalLag += (int)lag;
                                                    }
                                                    catch
                                                    {
                                                        // Skip partition if we can't get lag
                                                    }
                                                }

                                                string status = groupDescription.State switch
                                                {
                                                    ConsumerGroupState.Stable => "Active",
                                                    ConsumerGroupState.Dead => "Dead",
                                                    ConsumerGroupState.Empty => "Empty",
                                                    ConsumerGroupState.PreparingRebalance => "Rebalancing",
                                                    ConsumerGroupState.CompletingRebalance => "Rebalancing",
                                                    _ => "Unknown"
                                                };

                                                consumers.Add(new ConsumerInfo(
                                                    groupDescription.GroupId,
                                                    "no-member",
                                                    totalLag,
                                                    status));
                                            }

                                            consumer.Close();
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    // Log error for this group but continue with others
                                    Console.WriteLine($"Error processing consumer group {groupId}: {ex.Message}");
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // ListConsumerGroupsAsync might not be available in all Kafka versions
                // Log the error but don't fail completely
                Console.WriteLine($"Warning: Could not list consumer groups from broker {broker.ConnectionName}: {ex.Message}");
                Console.WriteLine("This might be due to Kafka version incompatibility or insufficient permissions.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error loading consumers from broker {broker.ConnectionName} ({broker.Host}:{broker.Port}): {ex.Message}");
            throw;
        }

        return consumers;
    }
}
