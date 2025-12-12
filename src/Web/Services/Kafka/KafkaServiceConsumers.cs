using DW.KafkaViwer.Web.Models;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace DW.KafkaViwer.Web.Services.Kafka;

public partial class KafkaService
{
    /// <summary>
    /// Loads detailed consumer group information from a broker.
    /// If topicName is null, returns all consumer groups.
    /// </summary>
    private List<ConsumerInfo> LoadConsumersFromBroker(BrokerInfo broker, string? topicName = null)
    {
        var consumers = new List<ConsumerInfo>();

        try
        {
            var adminConfig = CreateAdminClientConfig(broker);
            using var adminClient = new AdminClientBuilder(adminConfig).Build();

            try
            {
                var listResult = adminClient.ListConsumerGroupsAsync(
                    new ListConsumerGroupsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                ).Result;
                
                if (listResult?.Valid == null) return consumers;

                var groupIds = listResult.Valid.Select(g => g.GroupId).ToList();
                
                if (groupIds.Count == 0) return consumers;

                var describeResult = adminClient.DescribeConsumerGroupsAsync(
                    groupIds,
                    new DescribeConsumerGroupsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                ).Result.ConsumerGroupDescriptions.ToDictionary(x => x.GroupId);

                if (describeResult == null) return consumers;

                // Get offsets for all groups
                var offsetsRequests = groupIds.Select(g => new ConsumerGroupTopicPartitions(g, null)).ToList();
                var offsetsResult = adminClient.ListConsumerGroupOffsetsAsync(
                    offsetsRequests,
                    new ListConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                ).Result;

                var consumerConfig = CreateConsumerConfig(broker, $"lag-calculator-{Guid.NewGuid()}");
                using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

                foreach (var groupId in groupIds)
                {
                    try
                    {
                        if (!describeResult.TryGetValue(groupId, out var groupDescription) || groupDescription == null)
                            continue;

                        // Find offsets for this group
                        var groupOffsets = offsetsResult.FirstOrDefault(o => o.Group == groupId);
                        if (groupOffsets?.Partitions == null) continue;

                        // Filter by topic if specified
                        var relevantPartitions = topicName == null
                            ? groupOffsets.Partitions.ToList()
                            : groupOffsets.Partitions.Where(p => p.Topic == topicName).ToList();

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
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error processing consumer group {groupId}: {ex.Message}");
                    }
                }

                consumer.Close();
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
}

