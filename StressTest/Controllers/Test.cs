using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;

namespace StressTest.Controllers;

[ApiController]
[Route("[controller]")]
public class Test : ControllerBase
{
    private readonly PointInfoSubmitter _pointInfoSubmitter;
    private readonly IMemoryCache _cache;

    private static readonly SemaphoreSlim Semaphore = new(1, 1);

    public Test(PointInfoSubmitter pointInfoSubmitter, IMemoryCache cache)
    {
        _pointInfoSubmitter = pointInfoSubmitter;
        _cache = cache;
    }

    [HttpPost("NewApiWithSemaphore/{index:int}")]
    public async Task<IActionResult> NewApiWithSemaphore(int index, PointRequest pointRequest, CancellationToken ct)
    {
        await Semaphore.WaitAsync(ct);

        if (_cache.TryGetValue(index, out _)) return Ok();

        _cache.Set(index, 0);

        try
        {
            await _pointInfoSubmitter
                .ExecuteSpNew(pointRequest.PointData)
                .ExecuteWithRetry(null, HttpContext.RequestAborted, ct);

            _cache.Remove(index);
            Semaphore.Release();
            return Ok();
        }
        catch
        {
            Semaphore.Release();
            throw;
        }
    }
}