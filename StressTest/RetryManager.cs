using System.Diagnostics;

namespace StressTest;

public static class RetryManager
{
    public static async Task ExecuteWithRetry(
        this Task task,
        TimeSpan? time = null,
        CancellationToken contextCt = default,
        CancellationToken actionCt = default)
    {
        time ??= TimeSpan.FromSeconds(30);

        var stopwatch = Stopwatch.StartNew();

        Exception lastException;

        try
        {
            await task;
            return;
        }
        catch (Exception ex)
        {
            lastException = ex;
        }

        stopwatch.Stop();

        if (stopwatch.Elapsed > time)
            return;

        if (contextCt.IsCancellationRequested || actionCt.IsCancellationRequested)
            throw lastException;

        await task;
    }
}