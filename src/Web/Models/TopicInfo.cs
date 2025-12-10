namespace DW.KafkaViwer.Web.Models;

public record TopicInfo(string Name, int Partitions, long Messages, int RetentionDays);

public record TopicFilter(string? Name);

