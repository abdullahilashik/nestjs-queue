# Phase 2: Production Infrastructure, Resilience & Worker Topologies

Before writing complex business features, your production foundation must be rock-solid. A production queue that drops jobs during a routine rolling deploy, or freezes because an image parser blocked the main thread, is not production-ready.

In this phase, we tackle the runtime physics of BullMQ in NestJS:
1. **Worker Process Topologies:** In-Process vs. Sandboxed Child Processes.
2. **Cluster & High-Availability:** Redis Cluster, Sentinel, and Redis Hash Tags `{...}`.
3. **Enterprise Observability:** Integrating Bull Board securely with NestJS.
4. **Lifecycle & Pod Drainage:** Handling Kubernetes `SIGTERM` and graceful shutdowns without losing in-flight jobs.

---

## 1. Worker Topologies: In-Process vs. Sandboxed Child Processes

### The Architectural Problem
Node.js executes JavaScript on a single thread via the V8 Event Loop. 

```
┌─────────────────────────────────────────────────────────────┐
│                     Node.js Single Thread                   │
│                                                             │
│   HTTP GET /api/users ────┐                                 │
│   HTTP POST /orders   ────┼──► [ Event Loop ] ──► BullMQ    │
│   Heavy CPU Task      ────┘        ▲              Heartbeat │
│   (e.g., PDF / Sharp)              │               (Timer)  │
│         │                          │                   │    │
│         ▼                          │                   ▼    │
│   BLOCKS FOR 8 SECONDS ────────────┴─────────────► FAILS!   │
│                                                   (Stalled) │
└─────────────────────────────────────────────────────────────┘
```

When an In-Process Worker executes a synchronous, CPU-intensive task (like image resizing via `sharp`, PDF rendering, parsing large spreadsheets, or crypto operations):
1. The **entire event loop blocks**.
2. HTTP requests stall and start timing out across the entire API instance.
3. BullMQ's internal heartbeat cannot execute to renew the lock in Redis.
4. Redis considers the worker dead and marks the job **Stalled**, moving it back to `wait` or `failed`.
5. When the CPU task finally finishes, the worker attempts to acknowledge the job, but Redis replies: *"You don't own this job anymore."* The job runs twice, or crashes.

---

### The Solution: Sandboxed Processors (`process.fork()`)

BullMQ supports **Sandboxed Processors**. Instead of running the job handler inside the NestJS main thread, BullMQ forks a dedicated Node.js child process per worker.

```
┌──────────────────────────────────────────────────────────┐
│                   NestJS Main Process                    │
│   - Receives HTTP Requests                               │
│   - Enqueues Jobs via BullMQ Queue                       │
│   - Manages connection lifecycle                         │
└────────┬─────────────────────────────────────────────────┘
         │ (ChildProcess.fork)
         ▼
┌──────────────────────────────────────────────────────────┐
│              Dedicated Worker Child Process              │
│   - Independent V8 Memory Space                          │
│   - Isolated Event Loop                                  │
│   - CPU blocking here CANNOT freeze the HTTP Server      │
│   - If it crashes (OOM/Segfault), the main app survives  │
└──────────────────────────────────────────────────────────┘
```

#### Production Implementation: Sandboxed Processor

##### Step 1: The Sandboxed Processor Function (Pure TypeScript/JavaScript file)
Create an isolated file that exports the default processor function. **Notice:** This file cannot use standard NestJS Dependency Injection directly because it runs in a separate child process.

```typescript
// src/processors/heavy-computation.sandbox.ts
import { Job } from 'bullmq';

export interface HeavyComputationData {
  datasetId: string;
  matrixSize: number;
}

export interface HeavyComputationResult {
  checksum: string;
  iterations: number;
  durationMs: number;
}

/**
 * Sandboxed Worker Entrypoint.
 * This function runs inside an isolated Node.js child process.
 */
export default async function (
  job: Job<HeavyComputationData, HeavyComputationResult>,
): Promise<HeavyComputationResult> {
  const startTime = Date.now();
  const { datasetId, matrixSize } = job.data;

  // Simulate heavy CPU computation (blocking loop)
  let accumulator = 0;
  const totalIterations = matrixSize * 10000;

  for (let i = 0; i < totalIterations; i++) {
    accumulator = (accumulator + Math.sqrt(i) * Math.sin(i)) % 1000000;

    // Report progress periodically without spamming Redis
    if (i % (totalIterations / 10) === 0) {
      const progress = Math.round((i / totalIterations) * 100);
      await job.updateProgress(progress);
    }
  }

  return {
    checksum: `chk_${accumulator.toFixed(4)}_${datasetId}`,
    iterations: totalIterations,
    durationMs: Date.now() - startTime,
  };
}
```

##### Step 2: Registering the Sandboxed Worker in NestJS
Point the `@Processor` to the compiled JavaScript path of the processor file.

```typescript
// src/heavy-computation/heavy-computation.worker.ts
import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger, OnModuleInit } from '@nestjs/common';
import { join } from 'path';

export const HEAVY_COMPUTATION_QUEUE = 'heavy-computation';

@Processor(HEAVY_COMPUTATION_QUEUE, {
  // Point to the compiled JS file path
  // In development (ts-node), this points to the .ts or transpiled .js
  useWorkerThreads: false, // false uses child_process.fork() (safer memory isolation)
  concurrency: 2,          // Spawn at most 2 child processes simultaneously
})
export class HeavyComputationWorker extends WorkerHost implements OnModuleInit {
  private readonly logger = new Logger(HeavyComputationWorker.name);

  onModuleInit() {
    this.logger.log('HeavyComputationWorker initialized in sandboxed mode.');
  }

  // NOTE: When running sandboxed processors, the process() method is 
  // omitted or overridden by BullMQ's child process runner.
  // The processor path must be configured via BullModule or Worker options.
  async process(): Promise<any> {
    // Process is handled by the sandboxed file.
  }
}
```

To bind the processor file path cleanly in NestJS, register the worker with its processor file location in the module:

```typescript
// src/heavy-computation/heavy-computation.module.ts
import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { join } from 'path';
import { HEAVY_COMPUTATION_QUEUE } from './heavy-computation.worker';
import { HeavyComputationWorker } from './heavy-computation.worker';

// Handle both development (ts-node/nest start) and compiled production (dist)
const processorPath = join(
  __dirname,
  '..',
  'processors',
  `heavy-computation.sandbox.${process.env.NODE_ENV === 'production' ? 'js' : 'ts'}`,
);

@Module({
  imports: [
    BullModule.registerQueue({
      name: HEAVY_COMPUTATION_QUEUE,
      processors: [
        {
          path: processorPath,
          concurrency: 2,
        },
      ],
    }),
  ],
  providers: [HeavyComputationWorker],
})
export class HeavyComputationModule {}
```

---

## 2. Cluster & High Availability: Redis Cluster & Hash Tags

When scaling Redis to enterprise scale, you move from a standalone Redis to **Redis Cluster** (sharded across multiple masters) or **Redis Sentinel** (master-replica with automated failover).

### The Redis Cluster Trap: CrossSlot Errors
Redis Cluster shards data across 16,384 **hash slots**. 
BullMQ uses atomic Lua scripts that touch multiple Redis keys simultaneously (e.g., `bull:queue:wait` and `bull:queue:active`).

**The Failure:** If Key A is hashed to Node 1, and Key B is hashed to Node 2, Redis rejects the atomic Lua script with:
```text
CROSSSLOT Keys in request don't hash to the same slot
```

### The Solution: Redis Hash Tags `{...}`
BullMQ supports **prefix hash tags**. By wrapping the queue prefix in curly braces `{...}`, Redis calculates the CRC16 hash slot using *only* the string inside the braces. This guarantees that **all keys belonging to that queue land on the exact same Redis node**, preventing `CROSSSLOT` errors completely.

```typescript
// src/config/bullmq-cluster.config.ts
import { BullRootModuleOptions } from '@nestjs/bullmq';
import { ConfigService } from '@nestjs/config';

export function getBullMQClusterConfig(
  configService: ConfigService,
): BullRootModuleOptions {
  const isCluster = configService.get<boolean>('REDIS_IS_CLUSTER', false);

  if (isCluster) {
    return {
      // The prefix MUST use Redis hash tags: {queue-prefix}
      prefix: '{bull_cluster}',
      connection: {
        // Pass array of nodes for cluster discovery
        clusters: [
          {
            host: configService.get<string>('REDIS_NODE_1_HOST', 'redis-1'),
            port: configService.get<number>('REDIS_NODE_1_PORT', 6379),
          },
          {
            host: configService.get<string>('REDIS_NODE_2_HOST', 'redis-2'),
            port: configService.get<number>('REDIS_NODE_2_PORT', 6379),
          },
          {
            host: configService.get<string>('REDIS_NODE_3_HOST', 'redis-3'),
            port: configService.get<number>('REDIS_NODE_3_PORT', 6379),
          },
        ],
        redisOptions: {
          password: configService.get<string>('REDIS_PASSWORD'),
          maxRetriesPerRequest: null, // REQUIRED for BullMQ
        },
      },
    };
  }

  // Fallback to Single Node / Sentinel
  return {
    prefix: 'bull',
    connection: {
      host: configService.get<string>('REDIS_HOST', 'localhost'),
      port: configService.get<number>('REDIS_PORT', 6379),
      password: configService.get<string>('REDIS_PASSWORD'),
      maxRetriesPerRequest: null,
      enableReadyCheck: false,
    },
  };
}
```

---

## 3. Enterprise Observability: Bull Board Setup

You cannot operate a queue in production blindly. You need an operational control plane to:
* Inspect active, waiting, and failed jobs.
* View stack traces of failures.
* Retry failed tasks with a single click.
* Dynamically pause/resume queues during upstream incidents.

We integrate **Bull Board** via `@bull-board/nestjs` and `@bull-board/express`.

### Installation
```bash
npm install @bull-board/nestjs @bull-board/api @bull-board/express
```

### Implementation with Route Protection Guard

```typescript
// src/observability/bull-board.module.ts
import { Module } from '@nestjs/common';
import { BullBoardModule } from '@bull-board/nestjs';
import { ExpressAdapter } from '@bull-board/express';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { BullModule } from '@nestjs/bullmq';
import { AUDIO_QUEUE_NAME } from '../audio/audio.constants';
import { HEAVY_COMPUTATION_QUEUE } from '../heavy-computation/heavy-computation.worker';
import { BasicAuthGuard } from './basic-auth.guard';

@Module({
  imports: [
    // 1. Initialize Bull Board Express Dashboard at /admin/queues
    BullBoardModule.forRoot({
      route: '/admin/queues',
      adapter: ExpressAdapter,
      middleware: [
        // In production, attach basic auth or session verification
        new BasicAuthGuard().use,
      ],
    }),

    // 2. Bind existing queues to the UI
    BullBoardModule.forFeature({
      name: AUDIO_QUEUE_NAME,
      adapter: BullMQAdapter,
    }),
    BullBoardModule.forFeature({
      name: HEAVY_COMPUTATION_QUEUE,
      adapter: BullMQAdapter,
    }),
  ],
})
export class DashboardModule {}
```

#### The Basic Auth Guard (Protecting Sensitive Operational Tools)

```typescript
// src/observability/basic-auth.guard.ts
import { Request, Response, NextFunction } from 'express';

export class BasicAuthGuard {
  use = (req: Request, res: Response, next: NextFunction) => {
    // Skip in local testing if needed, or enforce everywhere
    const authHeader = req.headers.authorization;
    if (!authHeader) {
      res.setHeader('WWW-Authenticate', 'Basic realm="BullBoard Admin"');
      return res.status(401).send('Authentication required.');
    }

    const auth = Buffer.from(authHeader.split(' ')[1] || '', 'base64')
      .toString()
      .split(':');
    const user = auth[0];
    const pass = auth[1];

    const adminUser = process.env.ADMIN_USER || 'admin';
    const adminPass = process.env.ADMIN_PASSWORD || 'ChangeMeInProd123!';

    if (user === adminUser && pass === adminPass) {
      return next();
    }

    res.setHeader('WWW-Authenticate', 'Basic realm="BullBoard Admin"');
    return res.status(401).send('Invalid credentials.');
  };
}
```

Now visiting `http://localhost:3000/admin/queues` prompts for credentials and gives you full operational visibility into your queues.

---

## 4. Lifecycle & Graceful Kubernetes Pod Drainage

### The Production Deploy Problem
When Kubernetes updates a deployment, it terminates old pods:
1. Kubernetes sends `SIGTERM` to the container.
2. If your NestJS app exits immediately (`process.exit(0)`), in-flight workers die abruptly.
3. Database transactions break midway, external API calls are left half-completed, and locks in Redis linger until timeout.
4. Kubernetes gives a pod a **termination grace period** (usually 30–60 seconds). You must use this window to stop taking new jobs and complete in-flight ones.

```
Kubernetes                 NestJS App                     Active Jobs
    │                           │                              │
    ├──── SIGTERM ─────────────►│                              │
    │                           ├── 1. Stop taking NEW jobs    │
    │                           │   (worker.pause / close)     │
    │                           │                              │
    │                           ├── 2. Await in-flight jobs ──►│ Finishes
    │                           │                              │ work cleanly
    │                           ├── 3. Close Redis connections │
    │                           │                              │
    ├──── Pod Terminated ◄──────┴── process.exit(0)            ▼
```

### The Production Implementation: Graceful Worker Drainage

BullMQ's `WorkerHost` handles `close()` gracefully if you wire up NestJS lifecycle hooks (`OnModuleDestroy`).

```typescript
// src/common/graceful-worker.base.ts
import { WorkerHost } from '@nestjs/bullmq';
import { OnApplicationShutdown, Logger } from '@nestjs/common';

/**
 * Base Worker Host that guarantees graceful shutdown on SIGTERM / SIGINT
 */
export abstract class GracefulWorkerHost
  extends WorkerHost
  implements OnApplicationShutdown
{
  protected readonly abstract logger: Logger;

  /**
   * Called by NestJS when Kubernetes signals SIGTERM or SIGINT
   */
  async onApplicationShutdown(signal?: string): Promise<void> {
    this.logger.warn(
      `Received termination signal [${signal}]. Starting graceful worker drainage...`,
    );

    try {
      // 1. Tell BullMQ to stop picking up new jobs from the queue.
      // 2. Wait for currently active jobs to finish their processing.
      // 3. True waits for active jobs to complete before closing the connection.
      await this.worker.close(true);

      this.logger.log('All active jobs finished cleanly. Worker closed.');
    } catch (error) {
      this.logger.error('Error occurred while shutting down worker:', error);
    }
  }
}
```

#### Updating `main.ts` to Enable Shutdown Hooks
NestJS shuts down immediately unless shutdown hooks are explicitly enabled:

```typescript
// src/main.ts
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { Logger } from '@nestjs/common';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const logger = new Logger('Bootstrap');

  // CRITICAL FOR PRODUCTION:
  // Enables NestJS to catch SIGTERM, SIGINT, SIGHUP and call OnApplicationShutdown
  app.enableShutdownHooks();

  const port = process.env.PORT || 3000;
  await app.listen(port);
  logger.log(`Application is running on port ${port}`);
}
bootstrap();
```

---

## Architectural Summary of Phase 2

1. **Sandboxed Processors** prevent CPU starvation of the Node.js event loop by delegating work to child processes.
2. **Redis Hash Tags (`{...}`)** ensure multi-key operations land on the same Redis cluster slot, eliminating `CROSSSLOT` errors.
3. **Bull Board** provides monitoring and manual recovery capabilities behind authentication guards.
4. **Shutdown Hooks (`app.enableShutdownHooks()`)** coupled with `worker.close(true)` protect in-flight tasks from corruption during deployments.

---

### Progress Checkpoint

With our infrastructure, topology, and lifecycle guards secured, we are ready to build out real-world application patterns.

**Ready for the use cases?** Reply with **"Proceed"**, and we will tackle the first batch from Phase 3:
* **Use Case 01: Strict Idempotency & Deduplication** (Cryptographic job hashing and atomic locks).
* **Use Case 02: Third-Party API Rate Limiting** (Token-bucket egress control for Stripe/Shopify APIs).
* **Use Case 03: Sandboxed Workers for Heavy Media/CPU** (Practical FFmpeg/Image pipelines).