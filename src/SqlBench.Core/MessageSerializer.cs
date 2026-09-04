using System.Text.Json;
using System.Text.Json.Serialization;

namespace SqlBench.Core;

public static class MessageSerializer
{
    public static byte[] Serialize(BenchmarkMessage message) =>
        JsonSerializer.SerializeToUtf8Bytes(message, MessageJsonContext.Default.BenchmarkMessage);

    public static BenchmarkMessage Deserialize(ReadOnlyMemory<byte> bytes) =>
        JsonSerializer.Deserialize(bytes.Span, MessageJsonContext.Default.BenchmarkMessage)
        ?? throw new JsonException("The RabbitMQ payload did not contain a benchmark message.");
}

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    GenerationMode = JsonSourceGenerationMode.Default)]
[JsonSerializable(typeof(BenchmarkMessage))]
internal sealed partial class MessageJsonContext : JsonSerializerContext;
