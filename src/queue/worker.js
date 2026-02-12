const { parentPort } = require('worker_threads');

const DEFAULT_TIMEOUTS = {
  cpu: 30000,
  memory: 15000,
  custom: 10000,
  system: 1000,
};

const processors = {
  system: {
    ping: () => ({ pong: true }),
  },
  cpu: {
    findPrimes: (params) => {
      const max = Math.min(params.max || 1_000_000, 2_000_000);
      const sieve = new Array(max).fill(true);
      sieve[0] = false;
      sieve[1] = false;

      for (let i = 2; i * i < max; i++) {
        if (!sieve[i]) continue;
        for (let j = i * i; j < max; j += i) {
          sieve[j] = false;
        }
      }

      let count = 0;
      const first10 = [];
      const last10 = [];

      for (let i = 2; i < max; i++) {
        if (!sieve[i]) continue;

        count++;
        if (first10.length < 10) first10.push(i);
        if (last10.length === 10) last10.shift();
        last10.push(i);
      }

      return {
        primeCount: count,
        first10Primes: first10,
        last10Primes: last10,
      };
    },

    fibonacci: (params) => {
      const n = Math.min(params.n || 40, 45);
      let a = 0;
      let b = 1;

      for (let i = 0; i < n; i++) {
        const next = a + b;
        a = b;
        b = next;
      }

      return { n, result: a };
    },

    matrixMultiply: (params) => {
      const size = Math.min(params.size || 80, 120);
      const a = Array.from({ length: size }, () =>
        Array.from({ length: size }, () => Math.random()),
      );
      const b = Array.from({ length: size }, () =>
        Array.from({ length: size }, () => Math.random()),
      );
      const c = Array.from({ length: size }, () => Array(size).fill(0));

      for (let i = 0; i < size; i++) {
        for (let k = 0; k < size; k++) {
          const aik = a[i][k];
          for (let j = 0; j < size; j++) {
            c[i][j] += aik * b[k][j];
          }
        }
      }

      return {
        size,
        sample: c[0][0],
        operationCount: size * size * size,
      };
    },
  },

  memory: {
    largeArray: (params) => {
      const sizeInMB = Math.min(params.sizeInMB || 10, 256);
      const elementCount = Math.floor((sizeInMB * 1024 * 1024) / 8);
      const arr = new Array(elementCount).fill(0);
      let sum = 0;

      for (let i = 0; i < arr.length; i++) {
        const value = Math.random();
        arr[i] = value;
        sum += value;
      }

      return {
        arraySize: arr.length,
        sum,
        average: arr.length > 0 ? sum / arr.length : 0,
        memoryMB: Math.floor((arr.length * 8) / 1024 / 1024),
      };
    },

    objectCloning: (params) => {
      const depth = Math.min(params.depth || 4, 6);
      const width = Math.min(params.width || 30, 50);

      const createNestedObject = (currentDepth) => {
        if (currentDepth <= 0) {
          return { value: Math.random() };
        }

        const node = {};
        for (let i = 0; i < width; i++) {
          node[`prop${i}`] = createNestedObject(currentDepth - 1);
        }

        return node;
      };

      const original = createNestedObject(depth);
      const cloned = JSON.parse(JSON.stringify(original));

      return {
        objectDepth: depth,
        objectWidth: width,
        success: Boolean(cloned),
      };
    },
  },

  custom: {
    execute: (params, functionCode) => {
      if (!functionCode) {
        return { error: '실행할 함수 코드가 제공되지 않았습니다' };
      }

      try {
        const dynamicFunction = new Function(
          'params',
          `"use strict";\n${functionCode}`,
        );

        return dynamicFunction(params);
      } catch (error) {
        return {
          error:
            error instanceof Error
              ? `함수 실행 중 오류: ${error.message}`
              : '함수 실행 중 알 수 없는 오류',
        };
      }
    },
  },
};

function withTimeout(fn, timeout) {
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      reject(new Error(`작업 실행 시간이 ${timeout}ms를 초과했습니다`));
    }, timeout);

    try {
      const result = fn();
      clearTimeout(timeoutId);
      resolve(result);
    } catch (error) {
      clearTimeout(timeoutId);
      reject(error);
    }
  });
}

parentPort.on('message', async (data) => {
  const startedAt = Date.now();
  const operationName = `${data.type}:${data.operation}`;

  try {
    const { type, operation, params = {}, functionCode, timeout } = data;

    if (!type || !operation) {
      throw new Error('유효하지 않은 작업 요청: type과 operation이 필요합니다');
    }

    const processor = processors[type];
    if (!processor) {
      throw new Error(`지원되지 않는 작업 유형: ${type}`);
    }

    const operationFn = processor[operation];
    if (!operationFn) {
      throw new Error(`'${type}' 유형에서 지원되지 않는 작업: ${operation}`);
    }

    const timeoutValue = timeout || DEFAULT_TIMEOUTS[type] || 10000;
    const result = await withTimeout(
      () => operationFn(params, functionCode),
      timeoutValue,
    );

    if (type === 'system' && operation === 'ping') {
      parentPort.postMessage({ healthCheck: true, success: true, result });
      return;
    }

    parentPort.postMessage({
      success: true,
      result,
      operation: operationName,
      duration: Date.now() - startedAt,
    });
  } catch (error) {
    parentPort.postMessage({
      success: false,
      error: error instanceof Error ? error.message : '알 수 없는 오류',
      operation: operationName,
      duration: Date.now() - startedAt,
    });
  }
});

parentPort.postMessage({
  success: true,
  initialized: true,
  supportedTypes: Object.keys(processors),
  supportedOperations: Object.keys(processors).reduce((acc, type) => {
    acc[type] = Object.keys(processors[type]);
    return acc;
  }, {}),
});
