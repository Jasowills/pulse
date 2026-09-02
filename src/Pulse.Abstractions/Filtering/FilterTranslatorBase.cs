namespace Pulse.Abstractions.Filtering;

/// <summary>
/// Shared visitor for <see cref="FilterExpr"/> — handles <c>And</c>/<c>Or</c>/<c>Not</c> recursion
/// once, so providers only implement <see cref="TranslateCompare"/>. Depth: one place to fix
/// empty-clause handling, recursion, and unknown-node errors.
/// </summary>
public abstract class FilterTranslatorBase<TResult>
{
    public TResult Translate(FilterExpr expr)
    {
        ArgumentNullException.ThrowIfNull(expr);
        return TranslateCore(expr);
    }

    private TResult TranslateCore(FilterExpr expr)
    {
        return expr switch
        {
            FieldCompare compare => TranslateCompare(compare),
            And and => TranslateAnd(and),
            Or or => TranslateOr(or),
            Not not => TranslateNot(not),
            _ => throw new NotSupportedException($"Unsupported filter expression '{expr.GetType().Name}'."),
        };
    }

    protected virtual TResult TranslateAnd(And and)
        => and.Clauses.Count == 0 ? EmptyAnd() : CombineAnd(and.Clauses.Select(TranslateCore));

    protected virtual TResult TranslateOr(Or or)
        => or.Clauses.Count == 0 ? EmptyOr() : CombineOr(or.Clauses.Select(TranslateCore));

    protected virtual TResult TranslateNot(Not not)
        => Negate(TranslateCore(not.Clause));

    protected abstract TResult TranslateCompare(FieldCompare compare);

    // Provider-specific empty/combine/negate — allows Postgres "TRUE"/"FALSE", SQL "(1=1)" etc., Mongo And/Or.
    protected abstract TResult EmptyAnd();
    protected abstract TResult EmptyOr();
    protected abstract TResult CombineAnd(IEnumerable<TResult> clauses);
    protected abstract TResult CombineOr(IEnumerable<TResult> clauses);
    protected abstract TResult Negate(TResult clause);
}
