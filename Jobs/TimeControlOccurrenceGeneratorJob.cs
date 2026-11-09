using Moneta.Domain.Entities;
using Moneta.Domain.Enums;
using Moneta.Infra.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Quartz;

namespace Moneta.Scheduler.Jobs;

/// <summary>
/// Pre-generates today's TimeControlOccurrence rows from each Active TimeControlItem's schedule,
/// across every Module that uses the TimeControl pattern (not scoped to one module — this is
/// infrastructure, not a per-Routine handler). Runs on a short interval (not just at local
/// midnight) so it catches up whenever the worker happens to be running, without depending on it
/// being up at the exact instant the day rolls over — same resilience reasoning as
/// ScheduleSyncJob. Idempotent: never creates a duplicate for the same (item, scheduled instant).
///
/// Only handles items with a structured schedule (Hours/DaysOfWeek set, "modo simples") — items
/// saved via "modo avançado" (raw cron) are skipped; decomposing an arbitrary cron into "which
/// hours fire today" generically isn't attempted here.
/// </summary>
[DisallowConcurrentExecution]
public class TimeControlOccurrenceGeneratorJob(
    IServiceScopeFactory scopeFactory,
    ILogger<TimeControlOccurrenceGeneratorJob> logger
) : IJob
{
    private static readonly string[] DayOrder = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

    public async ValueTask Execute(IJobExecutionContext context, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var now = DateTimeOffset.Now;
        var today = DateOnly.FromDateTime(now.DateTime);
        var todayCode = DayOrder[(int)now.DayOfWeek];

        var items = await db.TimeControlItems
            .Where(i => i.Status == TimeControlItemStatus.Active && i.Hours != null)
            .ToListAsync(ct);

        var createdCount = 0;

        foreach (var item in items)
        {
            var days = item.DaysOfWeek?.Split(',', StringSplitOptions.RemoveEmptyEntries);
            var matchesToday = days is null || days.Length == 0 || days.Contains(todayCode);
            if (!matchesToday) continue;

            var hours = item.Hours!.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(int.Parse);

            foreach (var hour in hours)
            {
                // Npgsql only accepts Offset=0 (UTC) for timestamptz — construct the instant at
                // local wall-clock hour/minute (what the schedule means), then normalize to UTC
                // for storage/comparison. The instant itself doesn't change, only its representation.
                var scheduledAt = new DateTimeOffset(today.Year, today.Month, today.Day, hour, item.Minute, 0, now.Offset)
                    .ToUniversalTime();

                var exists = await db.TimeControlOccurrences.AnyAsync(
                    o => o.TimeControlItemId == item.Id && o.ScheduledAt == scheduledAt, ct);
                if (exists) continue;

                db.TimeControlOccurrences.Add(new TimeControlOccurrence
                {
                    Id = Guid.NewGuid(),
                    TimeControlItemId = item.Id,
                    ScheduledAt = scheduledAt,
                    Status = TimeControlOccurrenceStatus.Pending,
                });
                createdCount++;
            }
        }

        var missedCutoff = now.AddHours(-24).ToUniversalTime();
        var missedCount = await db.TimeControlOccurrences
            .Where(o => o.Status == TimeControlOccurrenceStatus.Pending && o.ScheduledAt < missedCutoff)
            .ExecuteUpdateAsync(setters => setters.SetProperty(o => o.Status, TimeControlOccurrenceStatus.Missed), ct);

        if (createdCount > 0)
        {
            await db.SaveChangesAsync(ct);
        }

        if (createdCount > 0 || missedCount > 0)
        {
            logger.LogInformation(
                "TimeControlOccurrenceGeneratorJob: {Created} ocorrência(s) gerada(s), {Missed} marcada(s) como perdida(s).",
                createdCount, missedCount);
        }
    }
}
