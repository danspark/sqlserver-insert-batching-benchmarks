using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace SqlBench.Core;

public static class WorkloadGenerator
{
    public const int ParentCount = 10_000;
    public static readonly DateTime BaseTimestamp = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    public static IReadOnlyList<BenchmarkMessage> Generate(
        int count,
        int seed,
        ParentDistribution distribution)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
        var random = new Random(seed);
        var rows = new BenchmarkMessage[count];

        for (int index = 0; index < count; index++)
        {
            int parentId = SelectParent(random, distribution);
            rows[index] = CreateMessage(index, seed, parentId, random);
        }

        return rows;
    }

    public static int GetSerializedSize(IReadOnlyList<BenchmarkMessage> messages)
    {
        if (messages.Count == 0)
        {
            return 0;
        }

        return (int)Math.Round(messages.Average(static message => MessageSerializer.Serialize(message).Length));
    }

    private static BenchmarkMessage CreateMessage(int index, int seed, int parentId, Random random)
    {
        byte[] identity = SHA256.HashData(Encoding.UTF8.GetBytes($"{seed}:{index}"));
        byte[] correlation = SHA256.HashData(Encoding.UTF8.GetBytes($"correlation:{seed}:{index / 25}"));
        byte[] payloadHash = identity[..16];

        return new BenchmarkMessage
        {
            MessageId = new Guid(identity.AsSpan(0, 16)),
            ParentId = parentId,
            CorrelationId = new Guid(correlation.AsSpan(0, 16)),
            OccurredAt = BaseTimestamp.AddMilliseconds(index),
            SequenceNo = index,
            CounterValue = BinaryPrimitives.ReadInt64LittleEndian(identity.AsSpan(16, 8)) & long.MaxValue,
            Priority = (short)random.Next(0, 32),
            Amount = decimal.Round((decimal)random.NextDouble() * 100_000m, 4),
            IsActive = (index & 1) == 0,
            Code = $"MSG-{index % 10_000:D5}-{identity[0]:X2}",
            Description = $"Deterministic benchmark message {index} for parent {parentId}",
            PayloadHash = payloadHash,
            OptionalNote = index % 5 == 0 ? null : $"seed={seed};bucket={index % 97}"
        };
    }

    private static int SelectParent(Random random, ParentDistribution distribution) => distribution switch
    {
        ParentDistribution.Uniform => random.Next(1, ParentCount + 1),
        ParentDistribution.ModerateSkew => random.NextDouble() < 0.8
            ? random.Next(1, 2_001)
            : random.Next(2_001, ParentCount + 1),
        ParentDistribution.HotParent => random.NextDouble() < 0.95
            ? 1
            : random.Next(2, ParentCount + 1),
        _ => throw new ArgumentOutOfRangeException(nameof(distribution))
    };
}
