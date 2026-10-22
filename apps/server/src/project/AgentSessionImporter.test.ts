import { expect, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type ApplicationStoredEvent,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import {
  CLAUDE_MODEL_SELECTION,
  CODEX_MODEL_SELECTION,
} from "../orchestration-v2/testkit/fixtures/shared.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as AgentSessionImporter from "./AgentSessionImporter.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const projectId = ProjectId.make("agent-session-import-project");
const timestamp = "2026-09-01T10:00:00.000Z";
const now = DateTime.makeUnsafe(timestamp);
const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const persistence = Layer.mergeAll(
  stores,
  EventSink.layer.pipe(Layer.provide(stores)),
  ProviderSessionRuntime.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
  IdAllocator.layer,
);

function candidate(
  modelSelection: typeof CODEX_MODEL_SELECTION,
  providerSessionId: string,
  provider: "codex" | "claudeAgent" = "codex",
) {
  const providerInstanceId = modelSelection.instanceId;
  return {
    _tag: "Importable" as const,
    source: {
      provider,
      providerInstanceId,
      providerSessionId,
      filePath: `/tmp/${providerInstanceId}/${providerSessionId}.jsonl`,
      size: 100,
      mtimeMs: 2,
      device: 3,
      inode: 4,
      birthtimeMs: 1,
    },
    thread: {
      source: provider,
      providerInstanceId,
      providerSessionId,
      title: "Imported thread",
      model: modelSelection.model,
      createdAt: timestamp,
      updatedAt: timestamp,
      messages: [
        { role: "user" as const, text: "Fix it", createdAt: timestamp },
        { role: "assistant" as const, text: "Fixed", createdAt: timestamp },
      ],
    },
  } satisfies AgentSessionScanner.AgentSessionRecentThread;
}

function importId(source: ReturnType<typeof candidate>) {
  return ThreadId.make(
    `import:${source.source.providerInstanceId}:${source.source.providerSessionId}`,
  );
}

const seedOwner = Effect.fn(function* (
  source: ReturnType<typeof candidate>,
  lifecycle: "active" | "archived" | "deleted" = "active",
) {
  const sink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const { providerInstanceId, providerSessionId, provider } = source.source;
  const threadId = ThreadId.make(`owner:${providerInstanceId}:${providerSessionId}`);
  const driver = ProviderDriverKind.make(provider);
  const providerThread: OrchestrationV2ProviderThread = {
    id: ids.derive.providerThread({
      driver,
      nativeThreadId: providerSessionId,
    }),
    driver,
    providerInstanceId,
    providerSessionId: null,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId: providerSessionId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const thread: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId,
    title: "Native thread",
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: source.thread.model },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThread.id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: lifecycle === "archived" ? now : null,
    deletedAt: lifecycle === "deleted" ? now : null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make(`${threadId}:created`),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: thread,
      },
      {
        id: EventId.make(`${threadId}:provider`),
        type: "provider-thread.updated",
        threadId,
        occurredAt: now,
        payload: providerThread,
      },
    ],
  });
  return threadId;
});

const makeImporter = Effect.fn(function* (
  candidates: ReadonlyArray<AgentSessionScanner.AgentSessionRecentThread>,
  beforeTransaction: Effect.Effect<void> = Effect.void,
) {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* EventStore.EventStoreV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const published: Array<ApplicationStoredEvent> = [];
  const withTransaction: typeof sql.withTransaction = (effect) =>
    beforeTransaction.pipe(Effect.andThen(sql.withTransaction(effect)));
  const importSql = new Proxy(sql, {
    get: (target, key, receiver) =>
      key === "withTransaction" ? withTransaction : Reflect.get(target, key, receiver),
  });
  const sinkLayer = Layer.fresh(EventSink.layer).pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient, importSql),
        Layer.succeed(ProjectionStore.ProjectionStoreV2, projections),
        Layer.succeed(EventStore.EventStoreV2, {
          ...events,
          publishCommitted: (batch) =>
            events.publishCommitted(batch).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  published.push(...batch);
                }),
              ),
            ),
        }),
      ),
    ),
  );
  const context = yield* Layer.build(
    AgentSessionImporter.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          sinkLayer,
          Layer.succeed(IdAllocator.IdAllocatorV2, yield* IdAllocator.IdAllocatorV2),
          Layer.succeed(
            ProviderSessionRuntime.ProviderSessionRuntimeRepository,
            yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository,
          ),
          Layer.mock(AgentSessionScanner.AgentSessionScanner)({
            recentThreads: () => Stream.fromIterable(candidates),
          }),
          Layer.mock(ProjectService.ProjectService)({
            getById: () =>
              Effect.succeed(
                Option.some({
                  id: projectId,
                  title: "Project",
                  workspaceRoot: "/workspace/project",
                  defaultModelSelection: null,
                  scripts: [],
                  createdAt: timestamp,
                  updatedAt: timestamp,
                  deletedAt: null,
                }),
              ),
          }),
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadRecords: (threadId, fields) =>
              projections
                .getThreadRecords(threadId, fields)
                .pipe(
                  Effect.mapError(
                    (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
                  ),
                ),
          }),
        ),
      ),
    ),
  );
  const importer = Context.get(context, AgentSessionImporter.AgentSessionImporter);
  return { run: importer.importRecentAgentThreads({ projectId }), published };
});

it.effect(
  "ignores owned sessions, including archived and deleted threads, but imports other instances and unowned sessions",
  () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const events = yield* EventStore.EventStoreV2;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const active = candidate(CODEX_MODEL_SELECTION, "active");
      const archived = candidate(
        CLAUDE_MODEL_SELECTION,
        "020e7f00-dce6-406b-8ec6-ed90498945d6",
        "claudeAgent",
      );
      const deleted = candidate(CODEX_MODEL_SELECTION, "deleted");
      const owners = [
        yield* seedOwner(active),
        yield* seedOwner(archived, "archived"),
        yield* seedOwner(deleted, "deleted"),
      ];
      const before = yield* Effect.forEach(owners, (id) =>
        projections.getThreadRecords(id, ["providerThreads"]),
      );
      const otherInstance = candidate(
        { ...CLAUDE_MODEL_SELECTION, instanceId: ProviderInstanceId.make("claude-other") },
        archived.source.providerSessionId,
        "claudeAgent",
      );
      const unowned = candidate(CODEX_MODEL_SELECTION, "unowned");
      const importer = yield* makeImporter([active, archived, deleted, otherInstance, unowned]);

      expect(yield* importer.run).toEqual({ importedCount: 2, skippedCount: 0 });
      for (const source of [active, archived, deleted]) {
        const threadId = importId(source);
        expect(yield* Effect.option(projections.getThread(threadId))).toEqual(Option.none());
        expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual([]);
        expect(yield* runtimes.getByThreadId({ threadId })).toEqual(Option.none());
      }
      expect(
        yield* Effect.forEach(owners, (id) =>
          projections.getThreadRecords(id, ["providerThreads"]),
        ),
      ).toEqual(before);
      expect(
        new Set(
          importer.published.flatMap((stored) =>
            "event" in stored ? [stored.event.threadId] : [],
          ),
        ),
      ).toEqual(new Set([importId(otherInstance), importId(unowned)]));
      for (const source of [otherInstance, unowned]) {
        const threadId = importId(source);
        const records = yield* projections.getThreadRecords(threadId, [
          "messages",
          "providerThreads",
        ]);
        expect(records.messages.map((message) => message.text)).toEqual(["Fix it", "Fixed"]);
        expect(records.thread.historyOrigin).toBe("v1_import");
        expect(records.thread.activeProviderThreadId).toBe(records.providerThreads[0]?.id);
        expect(records.providerThreads[0]?.nativeThreadRef?.nativeId).toBe(
          source.source.providerSessionId,
        );
        expect(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }))).toMatchObject({
          providerInstanceId: source.source.providerInstanceId,
          resumeCursor:
            source.source.provider === "codex"
              ? { threadId: source.source.providerSessionId }
              : { threadId, resume: source.source.providerSessionId },
          runtimePayload: { importedTranscripts: [source.source] },
        });
      }
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("rechecks ownership after the import reaches the commit transaction", () =>
  Effect.gen(function* () {
    const enteringTransaction = yield* Deferred.make<void>();
    const continueTransaction = yield* Deferred.make<void>();
    const source = candidate(CODEX_MODEL_SELECTION, "claimed-during-import");
    const importer = yield* makeImporter(
      [source],
      Deferred.succeed(enteringTransaction, undefined).pipe(
        Effect.andThen(Deferred.await(continueTransaction)),
      ),
    );
    const pending = yield* Effect.forkScoped(importer.run);
    yield* Deferred.await(enteringTransaction);
    // Commit ownership after all pre-transaction reads, before the import acquires SQLite.
    const ownerId = yield* seedOwner(source);
    yield* Deferred.succeed(continueTransaction, undefined);
    expect(yield* Fiber.join(pending)).toEqual({ importedCount: 0, skippedCount: 0 });
    expect(yield* importer.run).toEqual({ importedCount: 0, skippedCount: 0 });
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const events = yield* EventStore.EventStoreV2;
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    expect(yield* Effect.option(projections.getThread(importId(source)))).toEqual(Option.none());
    expect(yield* events.read({ threadId: importId(source) }).pipe(Stream.runCollect)).toEqual([]);
    expect(yield* runtimes.list()).toEqual([]);
    expect(importer.published).toEqual([]);
    expect(
      (yield* projections.getThreadRecords(ownerId, ["providerThreads"])).providerThreads[0]
        ?.appThreadId,
    ).toBe(ownerId);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect(
  "retries failed commits and runtime persistence without replaying committed history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const events = yield* EventStore.EventStoreV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const source = candidate(CODEX_MODEL_SELECTION, "retry");
      const threadId = importId(source);
      const importer = yield* makeImporter([source]);
      yield* sql`CREATE TEMP TRIGGER fail_import BEFORE INSERT ON orchestration_events WHEN NEW.event_type = 'message.updated' BEGIN SELECT RAISE(ABORT, 'test publication failure'); END`;
      expect(yield* importer.run).toEqual({ importedCount: 0, skippedCount: 1 });
      expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual([]);
      expect(yield* Effect.option(projections.getThread(threadId))).toEqual(Option.none());
      expect(yield* runtimes.list()).toEqual([]);
      expect(importer.published).toEqual([]);
      yield* sql`DROP TRIGGER fail_import`;

      yield* sql`CREATE TEMP TRIGGER fail_runtime BEFORE INSERT ON provider_session_runtime BEGIN SELECT RAISE(ABORT, 'test runtime failure'); END`;
      expect(yield* importer.run).toEqual({ importedCount: 0, skippedCount: 1 });
      const committed = yield* events.read({ threadId }).pipe(Stream.runCollect);
      expect(committed).toHaveLength(6);
      expect(importer.published).toHaveLength(6);
      expect(yield* runtimes.list()).toEqual([]);
      yield* sql`DROP TRIGGER fail_runtime`;

      expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual(committed);
      expect(importer.published).toHaveLength(6);
      expect(
        (yield* projections.getThreadRecords(threadId, ["messages"])).messages.map(
          (message) => message.text,
        ),
      ).toEqual(["Fix it", "Fixed"]);
      expect(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }))).toMatchObject({
        providerInstanceId: source.source.providerInstanceId,
        resumeCursor: { threadId: source.source.providerSessionId },
        runtimePayload: { importedTranscripts: [source.source] },
      });
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

function transcriptMessages(count: number) {
  return Array.from({ length: count }, (_, index) => ({
    role: index % 2 === 0 ? ("user" as const) : ("assistant" as const),
    text: `message ${index}`,
    createdAt: timestamp,
  }));
}

function continued(
  source: ReturnType<typeof candidate>,
  messages: ReturnType<typeof candidate>["thread"]["messages"],
  identity: { readonly size: number; readonly mtimeMs: number },
): ReturnType<typeof candidate> {
  return {
    ...source,
    source: { ...source.source, ...identity },
    thread: { ...source.thread, messages },
  };
}

const importedThreadState = Effect.fn(function* (threadId: ThreadId) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const events = yield* EventStore.EventStoreV2;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  return {
    records: yield* projections.getThreadRecords(threadId, ["messages", "turnItems"]),
    events: yield* events.read({ threadId }).pipe(Stream.runCollect),
    runtime: yield* runtimes.getByThreadId({ threadId }),
  };
});

it.effect(
  "refreshes appended messages without rewriting unchanged slots, and writes nothing on a retry",
  () =>
    Effect.gen(function* () {
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const initial = candidate(CODEX_MODEL_SELECTION, "continued");
      const threadId = importId(initial);
      yield* (yield* makeImporter([initial])).run;

      const next = continued(
        initial,
        [...initial.thread.messages, { role: "user", text: "One more", createdAt: timestamp }],
        { size: 200, mtimeMs: 5 },
      );
      const refresh = yield* makeImporter([next]);
      expect(yield* refresh.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect(
        refresh.published.flatMap((stored) => ("event" in stored ? [stored.event.type] : [])),
      ).toEqual(["message.updated", "turn-item.updated"]);
      const state = yield* importedThreadState(threadId);
      expect(state.records.messages.map((message) => [message.id, message.text])).toEqual([
        [`${threadId}:000000`, "Fix it"],
        [`${threadId}:000001`, "Fixed"],
        [`${threadId}:000002`, "One more"],
      ]);
      expect(state.records.turnItems.map((item) => item.ordinal)).toEqual([1, 2, 3]);
      expect(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }))).toMatchObject({
        runtimePayload: { importedTranscripts: [next.source] },
      });

      expect(yield* refresh.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect(refresh.published).toHaveLength(2);
      expect(yield* importedThreadState(threadId)).toEqual(state);
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("rewrites every slot in order when the retained window slides past 200", () =>
  Effect.gen(function* () {
    const transcript = transcriptMessages(205);
    const initial = continued(
      candidate(CODEX_MODEL_SELECTION, "sliding"),
      transcript.slice(0, 200),
      { size: 100, mtimeMs: 2 },
    );
    const threadId = importId(initial);
    yield* (yield* makeImporter([initial])).run;

    const window = [transcript[0]!, ...transcript.slice(-199)];
    const refresh = yield* makeImporter([continued(initial, window, { size: 300, mtimeMs: 9 })]);
    expect(yield* refresh.run).toEqual({ importedCount: 1, skippedCount: 0 });
    // Slot 0 keeps the first user message; every later slot moved by five.
    expect(refresh.published).toHaveLength(199 * 2);
    const { records } = yield* importedThreadState(threadId);
    expect(records.messages.map((message) => message.text)).toEqual(
      window.map((message) => message.text),
    );
    expect(records.messages.map((message) => message.role)).toEqual(
      window.map((message) => message.role),
    );
    expect(new Set(records.messages.map((message) => message.id)).size).toBe(200);
    expect(
      records.turnItems.map((item) => [item.ordinal, "text" in item ? item.text : null]),
    ).toEqual(window.map((message, index) => [index + 1, message.text]));
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

const appended = (source: ReturnType<typeof candidate>) =>
  continued(
    source,
    [...source.thread.messages, { role: "user", text: "Continued", createdAt: timestamp }],
    { size: 200, mtimeMs: 5 },
  );

const runCreated = (threadId: ThreadId) =>
  ({
    id: EventId.make(`${threadId}:run`),
    type: "run.created",
    threadId,
    occurredAt: now,
    payload: {
      id: RunId.make(`${threadId}:run`),
      threadId,
      ordinal: 1,
      providerInstanceId: CODEX_MODEL_SELECTION.instanceId,
      modelSelection: CODEX_MODEL_SELECTION,
      providerThreadId: null,
      userMessageId: MessageId.make(`${threadId}:prompt`),
      rootNodeId: null,
      activeAttemptId: null,
      status: "completed",
      requestedAt: now,
      startedAt: now,
      completedAt: now,
      checkpointId: null,
      contextHandoffId: null,
    },
  }) satisfies OrchestrationV2DomainEvent;

it.effect(
  "records imported threads continued in T3 or deleted without rewriting or skipping them",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const withRun = candidate(CODEX_MODEL_SELECTION, "continued-in-t3");
      const deleted = candidate(CODEX_MODEL_SELECTION, "deleted-after-import");
      yield* (yield* makeImporter([withRun, deleted])).run;
      const runThreadId = importId(withRun);
      const deletedThreadId = importId(deleted);
      const importedThread = yield* projections.getThread(deletedThreadId);
      yield* sink.write({
        events: [
          runCreated(runThreadId),
          {
            id: EventId.make(`${deletedThreadId}:deleted`),
            type: "thread.deleted",
            threadId: deletedThreadId,
            occurredAt: now,
            payload: { ...importedThread, deletedAt: now },
          },
        ],
      });
      const threadIds = [runThreadId, deletedThreadId];
      const history = (threadId: ThreadId) =>
        importedThreadState(threadId).pipe(
          Effect.map(({ records, events }) => ({ records, events })),
        );
      const before = yield* Effect.forEach(threadIds, history);

      const refresh = yield* makeImporter([appended(withRun), appended(deleted)]);
      expect(yield* refresh.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect(refresh.published).toEqual([]);
      expect(yield* Effect.forEach(threadIds, history)).toEqual(before);
      for (const [threadId, source] of [
        [runThreadId, appended(withRun)],
        [deletedThreadId, appended(deleted)],
      ] as const) {
        expect(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }))).toMatchObject({
          runtimePayload: { importedTranscripts: [source.source] },
        });
      }

      const recorded = yield* makeImporter(
        [withRun, deleted].map((source) => ({
          _tag: "AlreadyImported" as const,
          source: appended(source).source,
        })),
      );
      expect(yield* recorded.run).toEqual({ importedCount: 1, skippedCount: 0 });
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("leaves an imported thread untouched when T3 starts a run during a refresh", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const initial = candidate(CODEX_MODEL_SELECTION, "claimed-during-refresh");
    const threadId = importId(initial);
    yield* (yield* makeImporter([initial])).run;
    const before = yield* importedThreadState(threadId);

    const enteringTransaction = yield* Deferred.make<void>();
    const continueTransaction = yield* Deferred.make<void>();
    const refresh = yield* makeImporter(
      [appended(initial)],
      Deferred.succeed(enteringTransaction, undefined).pipe(
        Effect.andThen(Deferred.await(continueTransaction)),
      ),
    );
    const pending = yield* Effect.forkScoped(refresh.run);
    yield* Deferred.await(enteringTransaction);
    yield* sink.write({ events: [runCreated(threadId)] });
    yield* Deferred.succeed(continueTransaction, undefined);

    expect(yield* Fiber.join(pending)).toEqual({ importedCount: 0, skippedCount: 0 });
    expect(refresh.published).toEqual([]);
    const after = yield* importedThreadState(threadId);
    expect(after.records.messages).toEqual(before.records.messages);
    expect(after.records.turnItems).toEqual(before.records.turnItems);
    expect(after.runtime).toEqual(before.runtime);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("skips a transcript that shrank since it was imported", () =>
  Effect.gen(function* () {
    const initial = candidate(CODEX_MODEL_SELECTION, "shrunken");
    const threadId = importId(initial);
    yield* (yield* makeImporter([initial])).run;
    const before = yield* importedThreadState(threadId);

    const refresh = yield* makeImporter([
      continued(initial, initial.thread.messages.slice(0, 1), { size: 50, mtimeMs: 5 }),
    ]);
    expect(yield* refresh.run).toEqual({ importedCount: 0, skippedCount: 1 });
    expect(refresh.published).toEqual([]);
    expect(yield* importedThreadState(threadId)).toEqual(before);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);
