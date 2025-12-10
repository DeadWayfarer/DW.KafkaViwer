namespace DW.KafkaViwer.Web.Models;

public record TopicMessageFilter(
    string TopicName,
    string SearchType = "newest",
    int? Limit = null,
    DateTime? From = null,
    DateTime? To = null,
    string? Query = null);

public record TopicMessageInfo(
    string Topic,
    int Partition,
    long Offset,
    string Key,
    string Value,
    DateTime TimestampUtc);

