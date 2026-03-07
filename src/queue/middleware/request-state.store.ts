interface RequestState {
  responded: boolean;
  cleanupTimer: ReturnType<typeof setTimeout>;
}

export class RequestStateStore {
  private readonly requestStateMap = new Map<string, RequestState>();

  constructor(private readonly requestStateTtlMs: number) {}

  register(requestId: string): void {
    const cleanupTimer = setTimeout(() => {
      this.requestStateMap.delete(requestId);
    }, this.requestStateTtlMs);

    this.requestStateMap.set(requestId, {
      responded: false,
      cleanupTimer,
    });
  }

  markResponded(requestId: string): void {
    const state = this.requestStateMap.get(requestId);
    if (!state) return;
    state.responded = true;
  }

  hasResponded(requestId: string): boolean {
    return this.requestStateMap.get(requestId)?.responded ?? false;
  }

  release(requestId: string): void {
    const state = this.requestStateMap.get(requestId);
    if (state) {
      clearTimeout(state.cleanupTimer);
    }
    this.requestStateMap.delete(requestId);
  }
}
