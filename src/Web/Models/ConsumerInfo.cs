namespace DW.KafkaViwer.Web.Models;

public record ConsumerFilter(string TopicName);

public record ConsumerInfo(
    string Group,
    string Member,
    int Lag,
    string Status);

