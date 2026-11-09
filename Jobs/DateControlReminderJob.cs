using Moneta.Application.Routines;
using Moneta.Domain.Entities;
using Moneta.Domain.Enums;
using Moneta.Infra.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Quartz;

namespace Moneta.Scheduler.Jobs;

/// <summary>
/// Driven by a single Routine (id arrives via the firing trigger's JobDataMap — see
/// ScheduleSyncJob, which creates one trigger per routine) and scoped to that routine's ModuleId, so
/// multiple DateControl modules (e.g. Financeiro and a future "Investimentos") never mix entries.
/// Promotes unpaid entries past their due date to Overdue (sweep scoped to the module, safe to
/// run from any routine firing for it), then evaluates the routine's ReminderRulesDto (see
/// RoutineService) against every Pending/Overdue entry on each tick:
///
/// - BeforeDueDays/BeforeDueTime: fires once, the first tick at/after that instant — deduped via
///   a DateControlReminderDispatch row (BeforeDue kind) so a check cadence that runs several
///   times a day doesn't repeat it.
/// - OnDueDate: fires once on the due date itself — same dedupe approach (OnDue kind).
/// - AfterDueRepeatMinutes: once the entry is overdue, fires again every N minutes until it's
///   marked Paid — deduped by comparing elapsed time since its own most recent dispatch row
///   (AfterDueRepeat kind), not a one-time flag.
///
/// Each tick's reminders are grouped into up to 3 Notifications (one per rule kind that actually
/// matched something), not one per entry, so a pile of due bills doesn't turn into a pile of
/// native OS toasts.
/// </summary>
[DisallowConcurrentExecution]
public class DateControlReminderJob(
    IServiceScopeFactory scopeFactory,
    ILogger<DateControlReminderJob> logger
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
        // Local, not UTC: backend/worker run on the user's own machine, so DateTime.Now already
        // reflects the right "today" for a bill due on the calendar date the user sees — using
        // UtcNow here flips to tomorrow a few hours early for anyone west of UTC.
        var now = DateTime.Now;
        var today = DateOnly.FromDateTime(now);

        var overdueCount = await db.DateControlEntries
            .Where(e => e.ModuleId == moduleId && e.Status == DateControlEntryStatus.Pending && e.DueDate < today)
            .ExecuteUpdateAsync(setters => setters.SetProperty(e => e.Status, DateControlEntryStatus.Overdue), ct);

        if (overdueCount > 0)
        {
            logger.LogInformation("{Count} lançamento(s) promovido(s) para Vencida.", overdueCount);
        }

        var rules = RoutineService.ReadReminderRules(routine);

        var execution = new RoutineExecution
        {
            Id = Guid.NewGuid(),
            RoutineId = routine.Id,
            StartedAt = DateTimeOffset.UtcNow,
            Status = RoutineExecutionStatus.Running,
        };
        db.RoutineExecutions.Add(execution);
        await db.SaveChangesAsync(ct);

        var beforeDue = new List<DateControlEntry>();
        var onDue = new List<DateControlEntry>();
        var afterDueRepeat = new List<DateControlEntry>();

        if (rules is not null)
        {
            var candidates = await db.DateControlEntries
                .Where(e =>
                    e.ModuleId == moduleId &&
                    (e.Status == DateControlEntryStatus.Pending || e.Status == DateControlEntryStatus.Overdue))
                .ToListAsync(ct);

            foreach (var entry in candidates)
            {
                if (await ShouldFireBeforeDueAsync(db, entry, rules, today, now, ct)) beforeDue.Add(entry);
                if (await ShouldFireOnDueAsync(db, entry, rules, today, ct)) onDue.Add(entry);
                if (await ShouldFireAfterDueRepeatAsync(db, entry, rules, today, ct)) afterDueRepeat.Add(entry);
            }
        }

        var messages = new[]
        {
            BuildMessage(beforeDue, "vence em breve", "vencem em breve"),
            BuildMessage(onDue, "vence hoje", "vencem hoje"),
            BuildMessage(afterDueRepeat, "continua vencida", "continuam vencidas"),
        }.Where(m => m is not null).Cast<string>().ToList();

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

        var firedCount = beforeDue.Count + onDue.Count + afterDueRepeat.Count;

        execution.FinishedAt = DateTimeOffset.UtcNow;
        execution.Status = RoutineExecutionStatus.Succeeded;
        execution.Result = $"{firedCount} lançamento(s) agrupado(s) em {messages.Count} lembrete(s).";
        await db.SaveChangesAsync(ct);

        // Tells the API's PostgresNotificationListener a row exists — worker and backend stay
        // decoupled, neither needs to know whether the other process is up.
        foreach (var id in createdNotificationIds)
        {
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"SELECT pg_notify('notifications_channel', {id.ToString()})", ct);
        }

        logger.LogInformation(
            "Execução concluída para \"{Title}\": {FiredCount} lançamento(s), {NotificationCount} lembrete(s).",
            routine.Title, firedCount, messages.Count);
    }

    private static async Task<bool> ShouldFireBeforeDueAsync(
        AppDbContext db, DateControlEntry entry, ReminderRulesDto rules, DateOnly today, DateTime now, CancellationToken ct)
    {
        if (rules.BeforeDueDays is not int days || rules.BeforeDueTime is not string timeStr) return false;
        if (!TimeOnly.TryParse(timeStr, out var time)) return false;

        var targetDate = entry.DueDate.AddDays(-days);
        if (targetDate > today) return false; // instant hasn't arrived yet
        if (targetDate == today && TimeOnly.FromDateTime(now) < time) return false; // today but too early

        var alreadyFired = await db.DateControlReminderDispatches.AnyAsync(
            d => d.DateControlEntryId == entry.Id && d.RuleKind == DateControlReminderRuleKind.BeforeDue, ct);
        if (alreadyFired) return false;

        db.DateControlReminderDispatches.Add(new DateControlReminderDispatch
        {
            Id = Guid.NewGuid(),
            DateControlEntryId = entry.Id,
            RuleKind = DateControlReminderRuleKind.BeforeDue,
        });
        return true;
    }

    private static async Task<bool> ShouldFireOnDueAsync(
        AppDbContext db, DateControlEntry entry, ReminderRulesDto rules, DateOnly today, CancellationToken ct)
    {
        if (!rules.OnDueDate) return false;
        if (entry.DueDate != today) return false;

        var alreadyFired = await db.DateControlReminderDispatches.AnyAsync(
            d => d.DateControlEntryId == entry.Id && d.RuleKind == DateControlReminderRuleKind.OnDue, ct);
        if (alreadyFired) return false;

        db.DateControlReminderDispatches.Add(new DateControlReminderDispatch
        {
            Id = Guid.NewGuid(),
            DateControlEntryId = entry.Id,
            RuleKind = DateControlReminderRuleKind.OnDue,
        });
        return true;
    }

    private static async Task<bool> ShouldFireAfterDueRepeatAsync(
        AppDbContext db, DateControlEntry entry, ReminderRulesDto rules, DateOnly today, CancellationToken ct)
    {
        if (rules.AfterDueRepeatMinutes is not int minutes) return false;
        if (entry.DueDate >= today) return false; // not actually overdue yet

        var lastDispatch = await db.DateControlReminderDispatches
            .Where(d => d.DateControlEntryId == entry.Id && d.RuleKind == DateControlReminderRuleKind.AfterDueRepeat)
            .OrderByDescending(d => d.FiredAt)
            .FirstOrDefaultAsync(ct);

        if (lastDispatch is not null && (DateTimeOffset.UtcNow - lastDispatch.FiredAt).TotalMinutes < minutes) return false;

        db.DateControlReminderDispatches.Add(new DateControlReminderDispatch
        {
            Id = Guid.NewGuid(),
            DateControlEntryId = entry.Id,
            RuleKind = DateControlReminderRuleKind.AfterDueRepeat,
        });
        return true;
    }

    private static string? BuildMessage(List<DateControlEntry> entries, string singularLabel, string pluralLabel)
    {
        if (entries.Count == 0) return null;

        var total = entries.Sum(e => e.Amount);
        var noun = entries.Count == 1 ? "conta" : "contas";
        var label = entries.Count == 1 ? singularLabel : pluralLabel;
        return $"{entries.Count} {noun} {label} — R$ {total:0.00}";
    }

}
