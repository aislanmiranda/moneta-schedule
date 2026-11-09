using Moneta.Domain.Auth;
using Moneta.Infra;
using Moneta.Scheduler.Jobs;
using Quartz;

var builder = Host.CreateApplicationBuilder(args);

// The worker serves every user's routines — it sees all rows (see AppDbContext query filters).
builder.Services.AddSingleton<ICurrentUser, SystemCurrentUser>();

builder.Services.AddInfra(builder.Configuration);

builder.Services.AddQuartz(q =>
{
    // StoreDurably: these jobs have no trigger of their own — ScheduleSyncJob creates one
    // per Routine (read from Postgres) and keeps it in sync; a durable job survives with none.
    q.AddJob<DateControlReminderJob>(opts => opts.WithIdentity("DateControlReminderJob").StoreDurably());
    q.AddJob<TimeControlReminderJob>(opts => opts.WithIdentity("TimeControlReminderJob").StoreDurably());
    q.AddJob<DateTimeControlReminderJob>(opts => opts.WithIdentity("DateTimeControlReminderJob").StoreDurably());

    var syncJobKey = new JobKey("ScheduleSyncJob");
    q.AddJob<ScheduleSyncJob>(opts => opts.WithIdentity(syncJobKey));
    q.AddTrigger(t => t
        .ForJob(syncJobKey)
        .WithIdentity("ScheduleSyncJob-trigger")
        .StartNow()
        .WithSimpleSchedule(s => s.WithInterval(TimeSpan.FromSeconds(30)).RepeatForever()));

    // Infrastructure job, not a user-configurable Routine — owns its own fixed trigger
    // instead of going through ScheduleSyncJob. Short interval so it catches up whenever the
    // worker happens to be running, rather than depending on being up at local midnight. Covers
    // every Module using the TimeControl pattern, not just Vida Saudável.
    var timeControlGeneratorKey = new JobKey("TimeControlOccurrenceGeneratorJob");
    q.AddJob<TimeControlOccurrenceGeneratorJob>(opts => opts.WithIdentity(timeControlGeneratorKey));
    q.AddTrigger(t => t
        .ForJob(timeControlGeneratorKey)
        .WithIdentity("TimeControlOccurrenceGeneratorJob-trigger")
        .StartNow()
        .WithSimpleSchedule(s => s.WithInterval(TimeSpan.FromMinutes(10)).RepeatForever()));
});

builder.Services.AddQuartzHostedService(opts => opts.WaitForJobsToComplete = true);

var host = builder.Build();
host.Run();
