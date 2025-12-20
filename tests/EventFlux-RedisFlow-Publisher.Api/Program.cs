using EventFlux.Abstractions;
using EventFlux.RedisFlow;
using EventFlux.RedisFlow.Redis;
using EventFlux_RedisFlow_Publisher.Api.Events;
using Newtonsoft.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();

builder.Services.AddSwaggerGen();

builder.Services.AddRedisEventQueue(builder.Configuration, typeof(Program).Assembly);

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapPost("/publish", async (PublishEventRequest req, IRedisStreamPublisher publisher) =>
{
    var payload = JsonConvert.SerializeObject(req);
    await publisher.PublishAsync(nameof(PublishEventRequest), payload);
    return Results.Ok();
}).WithOpenApi();

app.MapPost("/publish-sendevent", async (SendEventRequest req, IRedisStreamPublisher publisher) =>
{
    var payload = JsonConvert.SerializeObject(req);
    await publisher.PublishAsync(nameof(SendEventRequest), payload);
    return Results.Ok();
}).WithOpenApi();

app.MapPost("/publish-with-context", async (PublishEventRequest req, IRedisStreamPublisher publisher) =>
{
    var payload = JsonConvert.SerializeObject(req);
    var context = new EventFlux.RedisFlow.Abstractions.EventContext
    {
        Version = "1.2.3",
        Configs = new Dictionary<string, string>
        {
            { "TenantId", "1001" },
            { "Source", "ContextAPI" }
        }
    };

    await publisher.PublishAsync(nameof(PublishEventRequest), payload, context);
    return Results.Ok();
}).WithOpenApi();

app.Run();
