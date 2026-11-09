using Moneta.Domain.Enums;
using Moneta.Infra.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Quartz;

namespace Moneta.Scheduler.Jobs;

/// <summary>
/// Keeps Quartz triggers in sync with every Routine's CronExpression (one routine = one trigger,
/// keyed by the routine's id), and fires an immediate out-of-band run for a routine when its
/// TriggerRequestedAt is set (the "Executar agora" button). Runs on a short fixed interval so
/// either change takes effect without restarting this process.
///
/// Add an entry here whenever a new routine type gets a handler — this is the single place mapping
/// "which RoutineType" to "which job executes it".
/// </summary>
[DisallowConcurrentExecution]
public class ScheduleSyncJob(
    IServiceScopeFactory scopeFactory,
    ILogger<ScheduleSyncJob> logger
) : IJob
{
    private static readonly IReadOnlyDictionary<RoutineType, JobKey> JobKeyByType =
        new Dictionary<RoutineType, JobKey>
        {
            [RoutineType.DateControlReminder] = new JobKey("DateControlReminderJob"),
            [RoutineType.TimeControlReminder] = new JobKey("TimeControlReminderJob"),
            [RoutineType.DateTimeControlReminder] = new JobKey("DateTimeControlReminderJob"),
        };

    private const string TriggerPrefix = "routine-";
    private const string TriggerSuffix = "-cron-trigger";

    public async ValueTask Execute(IJobExecutionContext context, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var scheduler = context.Scheduler;

        var routines = await db.Routines.ToListAsync(ct);

        foreach (var routine in routines)
        {
            if (!JobKeyByType.TryGetValue(routine.Type, out var jobKey)) continue;

            var triggerKey = new TriggerKey($"{TriggerPrefix}{routine.Id}{TriggerSuffix}");
            var existingTrigger = await scheduler.GetTrigger(triggerKey, ct) as ICronTrigger;
            var data = new JobDataMap { ["RoutineId"] = routine.Id.ToString() };

            if (routine.TriggerRequestedAt is not null)
            {
                // "Executar agora" fires regardless of Status — the user asked for this run explicitly.
                await scheduler.TriggerJob(jobKey, data, ct);
                routine.TriggerRequestedAt = null;
                logger.LogInformation("Execução manual disparada para \"{Title}\" (\"Executar agora\").", routine.Title);
            }

            if (routine.Status != RoutineStatus.Active)
            {
                if (existingTrigger is not null)
                {
                    await scheduler.UnscheduleJob(triggerKey, ct);
                    logger.LogInformation("Agendamento removido para rotina \"{Title}\".", routine.Title);
                }
                routine.NextRunAt = null;
                continue;
            }

            if (existingTrigger is null || existingTrigger.CronExpressionString != routine.CronExpression)
            {
                var newTrigger = TriggerBuilder.Create()
                    .WithIdentity(triggerKey)
                    .ForJob(jobKey)
                    .WithCronSchedule(routine.CronExpression)
                    .UsingJobData(data)
                    .Build();

                if (existingTrigger is null)
                {
                    await scheduler.ScheduleJob(newTrigger, default, ct);
                    logger.LogInformation(
                        "Agendamento criado para \"{Title}\": \"{Cron}\".", routine.Title, routine.CronExpression);
                }
                else
                {
                    await scheduler.RescheduleJob(triggerKey, newTrigger, ct);
                    logger.LogInformation(
                        "Agendamento atualizado para \"{Title}\": \"{Cron}\".", routine.Title, routine.CronExpression);
                }
            }

            // Keep NextRunAt in sync so the UI can show "próxima execução".
            var currentTrigger = await scheduler.GetTrigger(triggerKey, ct);
            routine.NextRunAt = currentTrigger?.NextFireTimeUtc;
        }

        // Only writes the routines whose TriggerRequestedAt/NextRunAt actually changed.
        await db.SaveChangesAsync(ct);

        // Deleted routines never reappear in `routines` above, so the per-routine loop never gets
        // a chance to unschedule their trigger — Quartz's in-memory RAMJobStore knows nothing
        // about the DB delete. Sweep every cron trigger this job owns and drop the orphaned ones.
        var existingIds = routines.Select(r => r.Id).ToHashSet();
        var allTriggerKeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.AnyGroup(), ct);

        foreach (var triggerKey in allTriggerKeys)
        {
            if (!TryParseRoutineId(triggerKey.Name, out var orphanedId)) continue;
            if (existingIds.Contains(orphanedId)) continue;

            await scheduler.UnscheduleJob(triggerKey, ct);
            logger.LogInformation("Agendamento removido para rotina excluída ({RoutineId}).", orphanedId);
        }
    }

    private static bool TryParseRoutineId(string triggerName, out Guid routineId)
    {
        routineId = Guid.Empty;
        if (!triggerName.StartsWith(TriggerPrefix, StringComparison.Ordinal) ||
            !triggerName.EndsWith(TriggerSuffix, StringComparison.Ordinal))
        {
            return false;
        }

        var idPart = triggerName[TriggerPrefix.Length..^TriggerSuffix.Length];
        return Guid.TryParse(idPart, out routineId);
    }
}
