using MongoDB.Driver;

namespace Pulse.Mongo;

/// <summary>Classifies Mongo errors that mean a resume token can no longer be honored.</summary>
internal static class MongoErrors
{
    // Error codes: 260 InvalidResumeToken, 280 ChangeStreamFatalError, 286 ChangeStreamHistoryLost.
    private const int InvalidResumeToken = 260;
    private const int ChangeStreamFatalError = 280;
    private const int ChangeStreamHistoryLost = 286;

    public static bool IsResumeInvalid(MongoException ex)
    {
        if (ex.HasErrorLabel("ResumeTokenChanged"))
        {
            return true;
        }

        if (ex is MongoCommandException command)
        {
            if (command.Code is InvalidResumeToken or ChangeStreamFatalError or ChangeStreamHistoryLost)
            {
                return true;
            }

            if (command.ErrorMessage?.Contains("resume point not found", StringComparison.OrdinalIgnoreCase) == true)
            {
                return true;
            }

            if (command.ErrorMessage?.Contains("InvalidResumeToken", StringComparison.OrdinalIgnoreCase) == true)
            {
                return true;
            }

            // Code 9 (FailedToParse) is generic, but a "resume token ... not a valid" message
            // specifically means the resume token itself is unparseable.
            if (command.ErrorMessage?.Contains("resume token", StringComparison.OrdinalIgnoreCase) == true)
            {
                return true;
            }
        }

        return ex.InnerException is MongoException inner && IsResumeInvalid(inner);
    }
}
