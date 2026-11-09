using Moneta.Domain.Entities;
using Moneta.Domain.Enums;
using Moneta.Infra.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Quartz;

namespace Moneta.Scheduler.Jobs;

/// <summary>
/// Driven by a Routine (Type = TimeControlReminder, id via JobDataMap), scoped to that routine's
/// ModuleId. Reminds about every Pending TimeControlOccurrence of that module whose ScheduledAt
/// has already passed — grouped by the item's TimeControlType (user-defined: Remédio, Água...),
/// one Notification per non-empty group. Same "don't flood with one toast per item" reasoning as
/// DateControlReminderJob.
/// </summary>
[DisallowConcurrentExecution]
public class TimeControlReminderJob(
    IServiceScopeFactory scopeFactory,
    ILogger<TimeControlReminderJob> logger
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

        var dueOccurrences = await db.TimeControlOccurrences
            .Include(o => o.TimeControlItem)
            .ThenInclude(i => i.TimeControlType)
            .Where(o =>
                o.TimeControlItem.ModuleId == moduleId &&
                o.Status == TimeControlOccurrenceStatus.Pending &&
                o.ScheduledAt <= now)
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

        var messages = dueOccurrences
            .GroupBy(o => o.TimeControlItem.TimeControlType.Name)
            .OrderBy(g => g.Key)
            .Select(g => BuildMessage(g.Key, g.ToList()))
            .ToList();

        var createdNotificationIds = new List<Guid>();

        foreach (var message in messages)
        {
            var notification = new Notification
            {
                Id = Guid.NewGuid(),
                ExecutionId = execution.Id,
                Message = message,
                DeliveredAt = DateTimeOffset.UtcNow,
            };
            db.Notifications.Add(notification);
            createdNotificationIds.Add(notification.Id);
        }

        execution.FinishedAt = DateTimeOffset.UtcNow;
        execution.Status = RoutineExecutionStatus.Succeeded;
        execution.Result = $"{dueOccurrences.Count} ocorrência(s) agrupada(s) em {messages.Count} lembrete(s).";
        await db.SaveChangesAsync(ct);

        // Tells the API's PostgresNotificationListener a row exists — worker and backend stay
        // decoupled, neither needs to know whether the other process is up.
        foreach (var id in createdNotificationIds)
        {
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"SELECT pg_notify('notifications_channel', {id.ToString()})", ct);
        }

        logger.LogInformation(
            "Execução concluída para \"{Title}\": {Count} ocorrência(s), {NotificationCount} lembrete(s).",
            routine.Title, dueOccurrences.Count, messages.Count);
    }

    private static string BuildMessage(string typeName, List<TimeControlOccurrence> occurrences)
    {
        var label = occurrences.Count == 1 ? "pendente" : "pendentes";
        var titles = string.Join(", ", occurrences.Select(o => o.TimeControlItem.Title).Distinct());
        return $"{occurrences.Count} {label} em {typeName}: {titles}";
    }
}
