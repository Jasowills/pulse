using Pulse.Abstractions;
using Pulse.Server;

namespace Pulse.Server.Tests;

public sealed class DictionaryFilterMatcherTests
{
    private static readonly DictionaryFilterMatcher Matcher = DictionaryFilterMatcher.Instance;

    private static IReadOnlyDictionary<string, object?> Doc(params (string Key, object? Value)[] fields)
        => fields.ToDictionary(f => f.Key, f => f.Value);

    [Fact]
    public void NullWhere_MatchesEverything()
    {
        Assert.True(Matcher.Matches(Doc(("status", "shipped")), new SubscriptionFilter("orders", null)));
    }

    [Theory]
    [InlineData("pending", true)]
    [InlineData("shipped", false)]
    public void Eq_MatchesByValue(string status, bool expected)
    {
        var filter = new SubscriptionFilter("orders", new FieldCompare("status", CompareOp.Eq, "pending"));
        Assert.Equal(expected, Matcher.Matches(Doc(("status", status)), filter));
    }

    [Fact]
    public void Eq_CoercesIntVsLong()
    {
        var filter = new SubscriptionFilter("orders", new FieldCompare("total", CompareOp.Eq, 100L));
        Assert.True(Matcher.Matches(Doc(("total", 100)), filter));
        Assert.False(Matcher.Matches(Doc(("total", 99)), filter));
    }

    [Fact]
    public void Eq_CoercesIntVsDouble()
    {
        var filter = new SubscriptionFilter("orders", new FieldCompare("total", CompareOp.Eq, 100.0));
        Assert.True(Matcher.Matches(Doc(("total", 100)), filter));
    }

    [Fact]
    public void MissingField_EqFails_NePasses()
    {
        Assert.False(Matcher.Matches(Doc(), new SubscriptionFilter("orders", new FieldCompare("status", CompareOp.Eq, "pending"))));
        Assert.True(Matcher.Matches(Doc(), new SubscriptionFilter("orders", new FieldCompare("status", CompareOp.Ne, "pending"))));
    }

    [Theory]
    [InlineData(50, false)]
    [InlineData(100, true)]
    [InlineData(150, true)]
    public void Gte_ComparesNumerically(int total, bool expected)
    {
        var filter = new SubscriptionFilter("orders", new FieldCompare("total", CompareOp.Gte, 100L));
        Assert.Equal(expected, Matcher.Matches(Doc(("total", total)), filter));
    }

    [Theory]
    [InlineData("z", false)]
    [InlineData("a", true)]
    [InlineData("m", true)]
    public void Lt_ComparesStringsOrdinal(string name, bool expected)
    {
        var filter = new SubscriptionFilter("orders", new FieldCompare("name", CompareOp.Lt, "n"));
        Assert.Equal(expected, Matcher.Matches(Doc(("name", name)), filter));
    }

    [Fact]
    public void In_MatchesAnyCandidate()
    {
        var filter = new SubscriptionFilter("orders",
            new FieldCompare("status", CompareOp.In, new List<object?> { "pending", "shipped" }));
        Assert.True(Matcher.Matches(Doc(("status", "shipped")), filter));
        Assert.False(Matcher.Matches(Doc(("status", "cancelled")), filter));
    }

    [Fact]
    public void NotIn_MissingFieldPasses()
    {
        var filter = new SubscriptionFilter("orders",
            new FieldCompare("status", CompareOp.NotIn, new List<object?> { "pending" }));
        Assert.True(Matcher.Matches(Doc(), filter));
        Assert.False(Matcher.Matches(Doc(("status", "pending")), filter));
    }

    [Fact]
    public void Exists_RequiresPresentNonNullValue()
    {
        Assert.True(Matcher.Matches(Doc(("status", "pending")),
            new SubscriptionFilter("orders", new FieldCompare("status", CompareOp.Exists, null))));
        Assert.False(Matcher.Matches(Doc(("status", (object?)null)),
            new SubscriptionFilter("orders", new FieldCompare("status", CompareOp.Exists, null))));
        Assert.False(Matcher.Matches(Doc(),
            new SubscriptionFilter("orders", new FieldCompare("status", CompareOp.Exists, null))));
    }

    [Fact]
    public void And_RequiresAllClauses()
    {
        var filter = new SubscriptionFilter("orders", new And(new FilterExpr[]
        {
            new FieldCompare("status", CompareOp.Eq, "pending"),
            new FieldCompare("total", CompareOp.Gte, 100L),
        }));
        Assert.True(Matcher.Matches(Doc(("status", "pending"), ("total", 100)), filter));
        Assert.False(Matcher.Matches(Doc(("status", "pending"), ("total", 50)), filter));
    }

    [Fact]
    public void Or_AcceptsAnyClause()
    {
        var filter = new SubscriptionFilter("orders", new Or(new FilterExpr[]
        {
            new FieldCompare("status", CompareOp.Eq, "pending"),
            new FieldCompare("status", CompareOp.Eq, "shipped"),
        }));
        Assert.True(Matcher.Matches(Doc(("status", "shipped")), filter));
        Assert.False(Matcher.Matches(Doc(("status", "cancelled")), filter));
    }

    [Fact]
    public void Not_Inverts()
    {
        var filter = new SubscriptionFilter("orders", new Not(new FieldCompare("status", CompareOp.Eq, "pending")));
        Assert.False(Matcher.Matches(Doc(("status", "pending")), filter));
        Assert.True(Matcher.Matches(Doc(("status", "shipped")), filter));
    }

    [Fact]
    public void NestedPath_DescendsDictionaries()
    {
        var filter = new SubscriptionFilter("orders",
            new FieldCompare("customer.address.city", CompareOp.Eq, "berlin"));
        var doc = Doc(("customer", Doc(("address", Doc(("city", "berlin"))))));
        Assert.True(Matcher.Matches(doc, filter));
        Assert.False(Matcher.Matches(Doc(("customer", Doc(("address", Doc(("city", "paris")))))), filter));
    }

    [Fact]
    public void NestedPath_ThroughArrays_MatchesAnyElement()
    {
        var filter = new SubscriptionFilter("orders", new FieldCompare("items.name", CompareOp.Eq, "pencil"));
        var doc = Doc(("items", new List<object?>
        {
            Doc(("name", "pen")),
            Doc(("name", "pencil")),
        }));
        Assert.True(Matcher.Matches(doc, filter));
    }

    [Fact]
    public void ArrayIndexPath_SelectsElement()
    {
        var filter = new SubscriptionFilter("orders", new FieldCompare("items.0.name", CompareOp.Eq, "pen"));
        var doc = Doc(("items", new List<object?>
        {
            Doc(("name", "pen")),
            Doc(("name", "pencil")),
        }));
        Assert.True(Matcher.Matches(doc, filter));
        Assert.False(Matcher.Matches(Doc(("items", new List<object?> { })), filter));
    }

    [Fact]
    public void In_OnListField_MatchesAnyElement()
    {
        var filter = new SubscriptionFilter("orders", new FieldCompare("tags", CompareOp.In, new List<object?> { "sale", "new" }));
        Assert.True(Matcher.Matches(Doc(("tags", new List<object?> { "sale" })), filter));
        Assert.False(Matcher.Matches(Doc(("tags", new List<object?> { "old" })), filter));
    }
}
