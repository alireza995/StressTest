using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;

namespace StressTest.Controllers;

[ApiController]
[Route("[controller]")]
public class Test : ControllerBase
{
    private readonly DbConnection _dbConnection;
    private readonly IMemoryCache _cache;

    private static readonly SemaphoreSlim Semaphore = new(1, 1);

    public Test(DbConnection dbConnection, IMemoryCache cache)
    {
        _dbConnection = dbConnection;
        _cache = cache;
    }

    [HttpPost("NewApiWithSemaphore/{index:int}")]
    public async Task<IActionResult> NewApiWithSemaphore(int index, PointRequest pointRequest, CancellationToken ct)
    {
        if (_cache.TryGetValue(index, out _)) 
            return Ok();

        _cache.Set(index, 0);
        await Semaphore.WaitAsync(ct);

        Exception? exception = null;
        try
        {
            await _dbConnection
                .ExecuteSpNew(pointRequest.PointData)
                .ExecuteWithRetry(null, HttpContext.RequestAborted, ct);
        }
        catch (Exception ex)
        {
            exception = ex;
        }
        finally
        {
            _cache.Remove(index);
            Semaphore.Release();
        }

        if (exception is null)
            return Ok();

        throw exception;
    }
}