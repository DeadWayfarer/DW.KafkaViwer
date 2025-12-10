var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorPages();
builder.Services.AddSingleton<DW.KafkaViwer.Web.Services.KafkaService>();

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
        new { id = "topic-list-view", title = "Topics" },
        new { id = "consumers", title = "Consumers" },
        new { id = "brokers", title = "Brokers" },
        new { id = "settings", title = "Settings" }
    };
    return Results.Json(items);
});

// Topics API backed by KafkaService with filtering
app.MapGet("/api/topics", (string? name, DW.KafkaViwer.Web.Services.KafkaService kafkaService) =>
{
    var topics = kafkaService.GetTopics(new DW.KafkaViwer.Web.Models.TopicFilter(name));
    return Results.Json(topics);
});

// Topic messages API
app.MapGet("/api/messages", (
    string topic,
    string? searchType,
    int? limit,
    DateTime? from,
    DateTime? to,
    string? query,
    DW.KafkaViwer.Web.Services.KafkaService kafkaService) =>
{
    if (string.IsNullOrWhiteSpace(topic))
    {
        return Results.BadRequest("topic is required");
    }

    var messages = kafkaService.GetTopicMessages(new DW.KafkaViwer.Web.Models.TopicMessageFilter(
        TopicName: topic,
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
    DW.KafkaViwer.Web.Services.KafkaService kafkaService) =>
{
    if (string.IsNullOrWhiteSpace(topic))
    {
        return Results.BadRequest("topic is required");
    }

    var items = kafkaService.GetConsumerInfo(new DW.KafkaViwer.Web.Models.ConsumerFilter(topic));
    return Results.Json(items);
});

// Brokers API
app.MapGet("/api/brokers", (DW.KafkaViwer.Web.Services.KafkaService kafkaService) =>
{
    var brokers = kafkaService.GetBrokers();
    return Results.Json(brokers);
});

app.MapPut("/api/brokers", (
    DW.KafkaViwer.Web.Models.BrokerInfo brokerInfo,
    DW.KafkaViwer.Web.Services.KafkaService kafkaService) =>
{
    kafkaService.UpdateBroker(brokerInfo);
    return Results.Ok();
});

app.Run();
