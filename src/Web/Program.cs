using DW.KafkaViwer.Web.Services.Kafka;
using DW.KafkaViwer.Web.Services;
using DW.KafkaViwer.Web.Components;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorPages();

// Configure JSON serialization to use camelCase
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase;
});

// Configure brokers from appsettings
var brokersSection = builder.Configuration.GetSection("Brokers");
var brokers = brokersSection.Get<List<DW.KafkaViwer.Web.Models.BrokerInfo>>() ?? new List<DW.KafkaViwer.Web.Models.BrokerInfo>();
var brokersDict = brokers.ToDictionary(x => x.Id);

builder.Services.AddSingleton<TopicCache>();
builder.Services.AddSingleton<ConsumerCache>();
builder.Services.AddSingleton<BrokersCache>(sp => new BrokersCache(brokersDict));
builder.Services.AddSingleton<KafkaService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();

app.UseAuthorization();

app.MapRazorPages();

// Mock nav data API
app.MapGet("/api/nav", () =>
{
    var items = new[]
    {
        new { id = "topic-list-view", title = "Топики" },
        new { id = "consumers", title = "Консьюмеры" },
        new { id = "brokers", title = "Брокеры" },
        new { id = "settings", title = "Настройки" }
    };
    return Results.Json(items);
});

// Topics API backed by KafkaService with filtering
app.MapGet("/api/topics", (string? name, KafkaService kafkaService) =>
{
    var topics = kafkaService.GetTopics(new DW.KafkaViwer.Web.Models.TopicFilter(name));
    return Results.Json(topics, new System.Text.Json.JsonSerializerOptions 
    { 
        PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase 
    });
});

// Topic messages API
app.MapGet("/api/messages", (
    string topic,
    int? brokerId,
    string? searchType,
    int? limit,
    DateTime? from,
    DateTime? to,
    string? query,
    KafkaService kafkaService) =>
{
    if (string.IsNullOrWhiteSpace(topic))
    {
        return Results.BadRequest("topic is required");
    }

    if (!brokerId.HasValue)
    {
        return Results.BadRequest("brokerId is required");
    }

    var messages = kafkaService.GetTopicMessages(new DW.KafkaViwer.Web.Models.TopicMessageFilter(
        TopicName: topic,
        BrokerId: brokerId.Value,
        SearchType: searchType ?? "newest",
        Limit: limit,
        From: from,
        To: to,
        Query: query));
    return Results.Json(messages);
});

// Consumers API
app.MapGet("/api/consumers", (
    string topic,
    KafkaService kafkaService) =>
{
    if (string.IsNullOrWhiteSpace(topic))
    {
        return Results.BadRequest("topic is required");
    }

    var items = kafkaService.GetConsumerInfo(new DW.KafkaViwer.Web.Models.ConsumerFilter(topic));
    return Results.Json(items);
});

// Brokers API
app.MapGet("/api/brokers", (KafkaService kafkaService) =>
{
    var brokers = kafkaService.GetBrokers().Values.ToList();
    return Results.Json(brokers);
});

app.MapPost("/api/brokers", (
    DW.KafkaViwer.Web.Models.BrokerInfo brokerInfo,
    KafkaService kafkaService) =>
{
    kafkaService.AddBroker(brokerInfo);
    return Results.Ok();
});

app.MapPut("/api/brokers", (
    DW.KafkaViwer.Web.Models.BrokerInfo brokerInfo,
    KafkaService kafkaService) =>
{
    kafkaService.UpdateBroker(brokerInfo);
    return Results.Ok();
});

app.MapDelete("/api/brokers/{id:int}", (
    int id,
    KafkaService kafkaService) =>
{
    var brokers = kafkaService.GetBrokers();
    var broker = brokers[id];
    if (broker == null)
    {
        return Results.NotFound();
    }
    kafkaService.DeleteBroker(broker);
    return Results.Ok();
});

// Send message API
app.MapPost("/api/messages", async (
    HttpRequest request,
    KafkaService kafkaService) =>
{
    try
    {
        var body = await request.ReadFromJsonAsync<SendMessageRequest>();
        if (body == null || string.IsNullOrWhiteSpace(body.Topic))
        {
            return Results.BadRequest("Topic is required");
        }

        // Get topic info
        var topics = kafkaService.GetTopics(new DW.KafkaViwer.Web.Models.TopicFilter(body.Topic));
        var topic = topics.FirstOrDefault();
        if (topic == null)
        {
            return Results.BadRequest($"Topic '{body.Topic}' not found");
        }

        // Create message info
        var message = new DW.KafkaViwer.Web.Models.TopicMessageInfo(
            Topic: body.Topic,
            Partition: 0, // Will be assigned by broker
            Offset: 0, // Will be assigned by broker
            Key: body.Key ?? string.Empty,
            Value: body.Value ?? "{}",
            TimestampUtc: DateTime.UtcNow);

        kafkaService.SendMessage(topic, message);
        return Results.Ok();
    }
    catch (Exception ex)
    {
        return Results.BadRequest($"Error sending message: {ex.Message}");
    }
});

app.Run();

// Request model for sending messages
public record SendMessageRequest(string Topic, string? Key, string Value);
