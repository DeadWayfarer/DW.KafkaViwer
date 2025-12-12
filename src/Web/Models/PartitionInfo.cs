namespace DW.KafkaViwer.Web.Models;

public record PartitionInfo(int PartitionId, long MinOffset, long MaxOffset);

public record TopicPartitionsInfo(string TopicName, int BrokerId, long TotalMessages, List<PartitionInfo> Partitions);

