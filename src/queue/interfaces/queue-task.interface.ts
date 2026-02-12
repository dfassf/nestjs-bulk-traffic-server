export interface QueueTask {
  id: number;
  requestId?: number | string;
  execute: () => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (reason?: Error | string) => void;
  timestamp: number;
  priority: number;
  category?: string;
  size?: number;
  isCpuIntensive?: boolean;
  isMemoryIntensive?: boolean;
  params?: Record<string, unknown>;
  functionCode?: string;
  timeout?: number;
}

export interface TaskBatch {
  tasks: QueueTask[];
  category: string;
  totalSize: number;
  createdAt: number;
}

export interface SerializedTask {
  id: number;
  requestId?: number | string;
  timestamp: number;
  priority: number;
  category?: string;
  size?: number;
}

export interface QueueSnapshot {
  timestamp: number;
  queues: {
    high: SerializedTask[];
    normal: SerializedTask[];
    low: SerializedTask[];
  };
  stats: {
    totalProcessed: number;
    totalRejected: number;
    totalTimeout: number;
    taskIdCounter: number;
  };
}

export interface WorkerTaskData {
  task: QueueTask;
  type?: string;
  operation?: string;
  params: Record<string, unknown>;
  functionCode?: string;
  timeout?: number;
}

export interface EnqueueOptions {
  priority?: number;
  requestId?: string | number;
  category?: string;
  batch?: boolean;
  size?: number;
  timeout?: number;
}
