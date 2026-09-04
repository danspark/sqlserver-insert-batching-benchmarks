using System.Text.Json;
using System.Text.Json.Serialization;

namespace SqlBench.Core;

public static class MessageSerializer
{
    public static JsonSerializerOptions Options { get; } = new(JsonSerializerDefaults.Web)
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false
    };

    public static byte[] Serialize(BenchmarkMessage message) =>
        JsonSerializer.SerializeToUtf8Bytes(message, Options);

    public static BenchmarkMessage Deserialize(ReadOnlyMemory<byte> bytes) =>
        JsonSerializer.Deserialize<BenchmarkMessage>(bytes.Span, Options)
        ?? throw new JsonException("The RabbitMQ payload did not contain a benchmark message.");
}
