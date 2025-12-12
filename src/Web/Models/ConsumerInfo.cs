namespace DW.KafkaViwer.Web.Models;

public record ConsumerFilter(string? TopicName);

public record PartitionLagInfo(
    string Topic,
    int Partition,
    long ConsumerOffset,
    long HighWatermark,
    long Lag);

public record MemberDetailInfo(
    string MemberId,
    string ClientId,
    string Host,
    List<PartitionLagInfo> Partitions);

public record ConsumerInfo(
    string Group,
    string Member,
    int Lag,
    string Status,
    int BrokerId,
    string BrokerName,
    List<MemberDetailInfo>? Members = null,
    List<PartitionLagInfo>? Partitions = null);
