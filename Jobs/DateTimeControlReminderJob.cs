using AgentOffice.Domain.Entities;
using AgentOffice.Domain.Enums;
using AgentOffice.Infra.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Quartz;

namespace AgentOffice.Scheduler.Jobs;

/// <summary>
/// Driven by a Routine (Type = DateTimeControlReminder, id via JobDataMap), scoped to that
/// routine's ModuleId. Marks Scheduled entries whose instant has already passed as Missed (sweep
/// scoped to the module), then reminds about every Scheduled entry whose ScheduledAt has already
/// passed but wasn't marked Done yet — same "don't flood with one toast per entry" grouping as
/// DateControlReminderJob/TimeControlReminderJob, here a single count+titles message since there's
/// no sub-type to group by (unlike TimeControl's Medication/Água/Academia).
/// </summary>
[DisallowConcurrentExecution]
public class DateTimeControlReminderJob(
    IServiceScopeFactory scopeFactory,
    ILogger<DateTimeControlReminderJob> logger
) : IJob
{
    public async ValueTask Execute(IJobExecutionContext context, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var routineIdRaw = context.MergedJobDataMap.GetString("RoutineId");
        if (!Guid.TryParse(routineIdRaw, out var routineId))
        {
            logger.LogWarning("Disparo sem RoutineId válido no JobDataMap — ignorado.");
            return;
        }

        var routine = await db.Routines.FirstOrDefaultAsync(r => r.Id == routineId, ct);

        if (routine is null)
        {
            logger.LogWarning("Rotina {RoutineId} não encontrada (excluída?) — disparo ignorado.", routineId);
            return;
        }

        var moduleId = routine.ModuleId;
        // Npgsql only accepts Offset=0 (UTC) for timestamptz query parameters.
        var now = DateTimeOffset.Now.ToUniversalTime();

        var dueEntries = await db.DateTimeControlEntries
            .Where(e =>
                e.ModuleId == moduleId &&
                e.Status == DateTimeControlEntryStatus.Scheduled &&
                e.ScheduledAt <= now)
            .ToListAsync(ct);

        var execution = new RoutineExecution
        {
            Id = Guid.NewGuid(),
            RoutineId = routine.Id,
            StartedAt = DateTimeOffset.UtcNow,
            Status = RoutineExecutionStatus.Running,
        };
        db.RoutineExecutions.Add(execution);
        await db.SaveChangesAsync(ct);

        var createdNotificationIds = new List<Guid>();

        if (dueEntries.Count > 0)
        {
            var label = dueEntries.Count == 1 ? "compromisso pendente" : "compromissos pendentes";
            var titles = string.Join(", ", dueEntries.Select(e => e.Title).Distinct());
            var notification = new Notification
            {
                Id = Guid.NewGuid(),
                ExecutionId = execution.Id,
                Message = $"{dueEntries.Count} {label}: {titles}",
                DeliveredAt = DateTimeOffset.UtcNow,
            };
            db.Notifications.Add(notification);
            createdNotificationIds.Add(notification.Id);
        }

        execution.FinishedAt = DateTimeOffset.UtcNow;
        execution.Status = RoutineExecutionStatus.Succeeded;
        execution.Result = $"{dueEntries.Count} compromisso(s) agrupado(s) em {createdNotificationIds.Count} lembrete(s).";
        await db.SaveChangesAsync(ct);

        // Tells the API's PostgresNotificationListener a row exists — worker and backend stay
        // decoupled, neither needs to know whether the other process is up.
        foreach (var id in createdNotificationIds)
        {
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"SELECT pg_notify('notifications_channel', {id.ToString()})", ct);
        }

        logger.LogInformation(
            "Execução concluída para \"{Title}\": {Count} compromisso(s), {NotificationCount} lembrete(s).",
            routine.Title, dueEntries.Count, createdNotificationIds.Count);
    }
}
