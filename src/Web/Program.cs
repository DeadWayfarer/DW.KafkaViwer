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
        new { id = "topic-view", title = "Topics" },
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

app.Run();
