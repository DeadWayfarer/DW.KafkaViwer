namespace DW.KafkaViwer.Web.Models;

public record TopicInfo(string Name, int Partitions, long Messages, int RetentionDays, int BrokerId, string BrokerName);

public record TopicFilter(string? Name);

