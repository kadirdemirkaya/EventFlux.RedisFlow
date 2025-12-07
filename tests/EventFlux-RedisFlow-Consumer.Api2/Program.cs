using EventFlux.RedisFlow;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();

builder.Services.AddSwaggerGen();

// Append a GUID to the consumer group so this service consumes independently
builder.Services.AddRedisEventQueue(builder.Configuration, assemblies: new[] { typeof(Program).Assembly }, appendGuidToConsumerGroup: true);

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.Run();

