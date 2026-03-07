import { Injectable } from '@nestjs/common';

@Injectable()
export class SimulationService {
  simulateCPU(iterations: number): Promise<{ hash: string; iterations: number }> {
    return new Promise((resolve) => {
      let hash = 0;
      for (let i = 0; i < iterations; i++) {
        hash = ((hash << 5) - hash + i) | 0;
      }
      resolve({ hash: hash.toString(16), iterations });
    });
  }

  simulateIO(delayMs: number): Promise<{ status: string; delayMs: number }> {
    return new Promise((resolve) => {
      setTimeout(() => resolve({ status: 'completed', delayMs }), delayMs);
    });
  }

  simulateBatch(itemCount: number): Promise<{ processed: number; items: string[] }> {
    return new Promise((resolve) => {
      const items: string[] = [];
      let done = 0;
      const processOne = () => {
        items.push(`item-${done}`);
        done++;
        if (done >= itemCount) {
          resolve({ processed: done, items: items.slice(0, 3) });
        } else {
          setTimeout(processOne, 5);
        }
      };
      processOne();
    });
  }
}
