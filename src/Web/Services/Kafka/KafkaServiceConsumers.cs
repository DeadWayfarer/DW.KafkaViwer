using DW.KafkaViwer.Web.Models;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace DW.KafkaViwer.Web.Services.Kafka;

public partial class KafkaService
{
    /// <summary>
    /// Loads detailed consumer group information from a broker.
    /// If groupIds is provided, loads only those groups. Otherwise loads all groups.
    /// </summary>
    private List<ConsumerInfo> LoadConsumersFromBroker(BrokerInfo broker, List<string>? groupIds = null)
    {
        var consumers = new List<ConsumerInfo>();

        try
        {

            try
            {
                var adminConfig = CreateAdminClientConfig(broker);
                using var adminClient = new AdminClientBuilder(adminConfig).Build();

                // If groupIds not provided, load all consumer groups from broker
                if (groupIds == null || groupIds.Count == 0)
                {
                    var listResult = adminClient.ListConsumerGroupsAsync(
                        new ListConsumerGroupsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                    ).Result;
                    
                    if (listResult?.Valid == null) return consumers;

                    groupIds = listResult.Valid.Select(g => g.GroupId).ToList();
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

    private List<ConsumerInfo> LoadConsumersFromBrokerByIds(BrokerInfo broker, Dictionary<string, ConsumerGroupDescription>? describeResult, List<string> groupIds)
    {
        var consumers = new List<ConsumerInfo>();

        var adminConfig = CreateAdminClientConfig(broker);
        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        var consumerConfig = CreateConsumerConfig(broker, $"lag-calculator-{Guid.NewGuid()}");
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

        foreach (var groupId in groupIds)
        {
            if (string.IsNullOrEmpty(groupId)) continue;
            
            try
            {
                if (describeResult == null || !describeResult.TryGetValue(groupId, out var groupDescription) || groupDescription == null)
                {
                    // Still create consumer info even if description is missing
                    consumers.Add(new ConsumerInfo(
                        groupId,
                        "unknown",
                        0,
                        "Unknown",
                        broker.Id,
                        broker.ConnectionName,
                        null,
                        null));
                    continue;
                }

                // Get offsets for this group only (Kafka API allows only one group at a time)
                var offsetsRequest = new ConsumerGroupTopicPartitions(groupId, null);
                var offsetsResult = adminClient.ListConsumerGroupOffsetsAsync(
                    new[] { offsetsRequest },
                    new ListConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                ).Result;

                var allPartitions = new List<PartitionLagInfo>();
                var members = new List<MemberDetailInfo>();
                int totalLag = 0;

                if (offsetsResult != null && offsetsResult.Count > 0)
                {
                    var groupOffsets = offsetsResult[0];
                    if (groupOffsets?.Partitions != null && groupOffsets.Partitions.Count > 0)
                    {
                        // Always load all partitions (topicName is used only for filtering in cache)
                        var relevantPartitions = groupOffsets.Partitions.ToList();

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
                            catch (Exception ex)
                            {
                                // Skip partition if we can't get lag, but still add partition info without lag
                                Console.WriteLine($"Warning: Could not get lag for partition {partition.Topic}:{partition.Partition}: {ex.Message}");
                                allPartitions.Add(new PartitionLagInfo(
                                    partition.Topic,
                                    partition.Partition,
                                    partition.Offset,
                                    0,
                                    0));
                            }
                        }
                    }
                }

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

                // Always create consumer info with partitions (even if empty) and members (even if empty)
                consumers.Add(new ConsumerInfo(
                    groupId,
                    groupDescription.Members?.Count > 0 ? $"{groupDescription.Members.Count} member(s)" : "no-member",
                    totalLag,
                    status,
                    broker.Id,
                    broker.ConnectionName,
                    members.Count > 0 ? members : null,
                    allPartitions.Count > 0 ? allPartitions : null));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing consumer group {groupId}: {ex.Message}");
                // Still create a basic consumer info entry even on error
                try
                {
                    if (!string.IsNullOrEmpty(groupId))
                    {
                        consumers.Add(new ConsumerInfo(
                            groupId,
                            "error",
                            0,
                            "Error",
                            broker.Id,
                            broker.ConnectionName,
                            null,
                            null));
                    }
                }
                catch { }
            }
        }
        
        // Consumer will be closed automatically by using statement
        return consumers;
    }

    public List<ConsumerInfo> GetConsumerInfo(ConsumerFilter filter)
    {
        // If no topic specified, return from cache
        if (string.IsNullOrWhiteSpace(filter.TopicName))
        {
            return _consumerCache.GetConsumers(null).ToList();
        }

        // If topic specified, load only groups that have partitions for this topic
        var consumers = new List<ConsumerInfo>();

        // Get group IDs from cache that have partitions for this topic
        var groupIdsByBroker = _consumerCache.GetGroupIdsByTopic(filter.TopicName);

        // If cache is empty or no groups found for this topic, return empty list
        // (Groups should be loaded at startup via LoadConsumerGroups)
        if (groupIdsByBroker.Count == 0)
        {
            Console.WriteLine($"No consumer groups found in cache for topic {filter.TopicName}. Cache may be empty or topic has no consumers.");
            return consumers;
        }

        // Iterate through all active brokers
        foreach (var broker in GetBrokers().Values)
        {
            if (broker.Status != "Active")
            {
                continue;
            }

            try
            {
                // Get group IDs for this broker that have partitions for the topic
                if (!groupIdsByBroker.TryGetValue(broker.Id, out var groupIds) || groupIds == null || groupIds.Count == 0)
                {
                    // No groups for this topic on this broker, skip
                    continue;
                }

                Console.WriteLine($"Loading {groupIds.Count} consumer groups for topic {filter.TopicName} from broker {broker.ConnectionName}");

                // Load only these specific groups from broker
                var brokerConsumers = LoadConsumersFromBroker(broker, groupIds);
                
                // Update cache with updated consumers from this broker (only for these groups)
                _consumerCache.AddConsumers(brokerConsumers);
                
                // Filter by topic for return
                var filteredConsumers = brokerConsumers
                    .Where(c => c.Partitions != null && c.Partitions.Any(p => p.Topic == filter.TopicName))
                    .Select(c =>
                    {
                        // Filter partitions and members by topic
                        var filteredPartitions = c.Partitions?.Where(p => p.Topic == filter.TopicName).ToList();
                        var filteredMembers = c.Members?.Select(m =>
                        {
                            var memberPartitions = m.Partitions?.Where(p => p.Topic == filter.TopicName).ToList() ?? new List<PartitionLagInfo>();
                            return new MemberDetailInfo(m.MemberId, m.ClientId, m.Host, memberPartitions);
                        }).Where(m => m.Partitions.Count > 0).ToList();

                        var totalLag = filteredPartitions?.Sum(p => p.Lag) ?? 0;

                        return new ConsumerInfo(
                            c.Group,
                            c.Member,
                            (int)totalLag,
                            c.Status,
                            c.BrokerId,
                            c.BrokerName,
                            filteredMembers?.Count > 0 ? filteredMembers : null,
                            filteredPartitions?.Count > 0 ? filteredPartitions : null);
                    })
                    .ToList();
                
                consumers.AddRange(filteredConsumers);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading consumers from broker {broker.ConnectionName} ({broker.Host}:{broker.Port}): {ex.Message}");
            }
        }

        return consumers;
    }
}

