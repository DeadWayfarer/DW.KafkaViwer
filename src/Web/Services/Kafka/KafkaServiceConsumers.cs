using DW.KafkaViwer.Web.Models;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace DW.KafkaViwer.Web.Services.Kafka;

public partial class KafkaService
{
    /// <summary>
    /// Loads detailed consumer group information from a broker.
    /// Always loads all consumer groups (topicName is ignored for loading, used only for filtering).
    /// </summary>
    private List<ConsumerInfo> LoadConsumersFromBroker(BrokerInfo broker, string? topicName = null)
    {
        var consumers = new List<ConsumerInfo>();

        try
        {

            try
            {
                var adminConfig = CreateAdminClientConfig(broker);
                using var adminClient = new AdminClientBuilder(adminConfig).Build();

                var groupIds = new List<string>();
                // If topicName is null, load all consumer groups
                if (topicName == null)
                {
                    var listResult = adminClient.ListConsumerGroupsAsync(
                        new ListConsumerGroupsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                    ).Result;
                    
                    if (listResult?.Valid == null) return consumers;

                    groupIds = listResult.Valid.Select(g => g.GroupId).ToList();
                }
                else {
                    groupIds = _consumerCache.GetConsumers(topicName).Select(c => c.Group).ToList();
                }
                
                if (groupIds.Count == 0) return consumers;

                var describeResult = adminClient.DescribeConsumerGroupsAsync(
                    groupIds,
                    new DescribeConsumerGroupsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                ).Result.ConsumerGroupDescriptions.ToDictionary(x => x.GroupId);

                if (describeResult == null) return consumers;

                consumers.AddRange(LoadConsumersFromBrokerByIds(broker, describeResult, groupIds));

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not list consumer groups from broker {broker.ConnectionName}: {ex.Message}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error loading consumers from broker {broker.ConnectionName} ({broker.Host}:{broker.Port}): {ex.Message}");
        }

        return consumers;
    }

    private List<ConsumerInfo> LoadConsumersFromBrokerByIds(BrokerInfo broker, Dictionary<string, ConsumerGroupDescription>? describeResult, List<string?> groupIds)
    {
        var consumers = new List<ConsumerInfo>();

        var adminConfig = CreateAdminClientConfig(broker);
        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        var consumerConfig = CreateConsumerConfig(broker, $"lag-calculator-{Guid.NewGuid()}");
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

        foreach (var groupId in groupIds)
        {
            try
            {
                // Get offsets for this group only (Kafka API allows only one group at a time)
                var offsetsRequest = new ConsumerGroupTopicPartitions(groupId, null);
                var offsetsResult = adminClient.ListConsumerGroupOffsetsAsync(
                    new[] { offsetsRequest },
                    new ListConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                ).Result;

                if (offsetsResult == null || offsetsResult.Count == 0) continue;

                var groupOffsets = offsetsResult[0];
                if (groupOffsets?.Partitions == null) continue;

                // Always load all partitions (topicName is used only for filtering in cache)
                var relevantPartitions = groupOffsets.Partitions.ToList();

                if (relevantPartitions.Count == 0) continue;

                // Group partitions by topic
                var partitionsByTopic = relevantPartitions.GroupBy(p => p.Topic).ToList();

                var allPartitions = new List<PartitionLagInfo>();
                var members = new List<MemberDetailInfo>();
                int totalLag = 0;

                // Calculate lag for each partition
                foreach (var partition in relevantPartitions)
                {
                    try
                    {
                        var topicPartition = new TopicPartition(partition.Topic, partition.Partition);
                        var watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(10));
                        
                        long consumerOffset = partition.Offset;
                        long highWatermark = watermarkOffsets.High;
                        long lag = Math.Max(0, highWatermark - consumerOffset);
                        
                        totalLag += (int)lag;
                        
                        allPartitions.Add(new PartitionLagInfo(
                            partition.Topic,
                            partition.Partition,
                            consumerOffset,
                            highWatermark,
                            lag));
                    }
                    catch
                    {
                        // Skip partition if we can't get lag
                    }
                }
                if (!describeResult.TryGetValue(groupId, out var groupDescription) || groupDescription == null)
                    continue;
                // Process members if available
                if (groupDescription.Members != null && groupDescription.Members.Count > 0)
                {
                    foreach (var member in groupDescription.Members)
                    {
                        if (member is MemberDescription memberDesc)
                        {
                            // Get partitions assigned to this member
                            var memberPartitions = memberDesc.Assignment?.TopicPartitions?.ToList() ?? new List<TopicPartition>();
                            var memberPartitionLags = new List<PartitionLagInfo>();

                            foreach (var tp in memberPartitions)
                            {
                                var partitionLag = allPartitions.FirstOrDefault(p => 
                                    p.Topic == tp.Topic && p.Partition == tp.Partition);
                                if (partitionLag != null)
                                {
                                    memberPartitionLags.Add(partitionLag);
                                }
                            }

                            // MemberDescription doesn't have MemberId property, use ClientId or Host as identifier
                            string memberId = memberDesc.ClientId ?? memberDesc.Host ?? "unknown";
                            
                            members.Add(new MemberDetailInfo(
                                memberId,
                                memberDesc.ClientId ?? "unknown",
                                memberDesc.Host ?? "unknown",
                                memberPartitionLags));
                        }
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

                // Create consumer info with details
                consumers.Add(new ConsumerInfo(
                    groupId,
                    groupDescription.Members?.Count > 0 ? $"{groupDescription.Members.Count} member(s)" : "no-member",
                    totalLag,
                    status,
                    broker.Id,
                    broker.ConnectionName,
                    members.Count > 0 ? members : null,
                    allPartitions.Count > 0 ? allPartitions : null));

                consumer.Close();

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing consumer group {groupId}: {ex.Message}");
            }
        }
        return consumers;
    }

    public List<ConsumerInfo> GetConsumerInfo(ConsumerFilter filter)
    {
        // If no topic specified, return from cache
        if (string.IsNullOrWhiteSpace(filter.TopicName))
        {
            return _consumerCache.GetConsumers(null).ToList();
        }

        // If topic specified, load directly from Kafka and update cache
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
                _consumerCache.AddConsumers(brokerConsumers);
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

