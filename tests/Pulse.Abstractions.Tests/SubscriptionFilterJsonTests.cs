using System.Text.Json;
using Pulse.Abstractions;

namespace Pulse.Abstractions.Tests;

public class SubscriptionFilterJsonTests
{
    private static readonly JsonSerializerOptions Options = new();

    [Fact]
    public void Serializes_ToSpecWireShape()
    {
        var filter = new SubscriptionFilter(
            "orders",
            new And(new FilterExpr[]
            {
                new FieldCompare("status", CompareOp.Eq, "pending"),
                new FieldCompare("total", CompareOp.Gte, 100L),
            }));

        var json = JsonSerializer.Serialize(filter, Options);

        Assert.Equal(
            """{"source":"orders","where":{"and":[{"field":"status","op":"eq","value":"pending"},{"field":"total","op":"gte","value":100}]}}""",
            json);
    }

    [Fact]
    public void Deserializes_SpecExampleJson()
    {
        var json = """
            {
              "source": "orders",
              "where": {
                "and": [
                  { "field": "status", "op": "eq", "value": "pending" },
                  { "field": "total", "op": "gte", "value": 100 }
                ]
              }
            }
            """;

        var filter = JsonSerializer.Deserialize<SubscriptionFilter>(json, Options);

        Assert.NotNull(filter);
        Assert.Equal("orders", filter.Source);
        var and = Assert.IsType<And>(filter.Where);
        Assert.Collection(
            and.Clauses,
            clause => Assert.Equal(new FieldCompare("status", CompareOp.Eq, "pending"), clause),
            clause => Assert.Equal(new FieldCompare("total", CompareOp.Gte, 100L), clause));
    }

    [Fact]
    public void MatchAll_NullWhere_RoundTrips()
    {
        var original = new SubscriptionFilter("orders", null);
        var json = JsonSerializer.Serialize(original, Options);

        Assert.Equal(original, JsonSerializer.Deserialize<SubscriptionFilter>(json, Options));
    }

    [Fact]
    public void RoundTrips_NestedFilter()
    {
        var original = new SubscriptionFilter(
            "inventory",
            new Or(new FilterExpr[]
            {
                new And(new FilterExpr[]
                {
                    new FieldCompare("qty", CompareOp.Gt, 0L),
                    new FieldCompare("price", CompareOp.Lte, 20L),
                }),
                new Not(new FieldCompare("discontinued", CompareOp.Eq, true)),
            }));

        var json = JsonSerializer.Serialize(original, Options);

        Assert.Equal(original, JsonSerializer.Deserialize<SubscriptionFilter>(json, Options));
    }
}
