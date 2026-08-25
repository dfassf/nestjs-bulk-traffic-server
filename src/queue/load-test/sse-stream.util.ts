import { Observable, defer, Subject } from 'rxjs';

/** SSE 로 내보낼 이벤트를 만드는 함수. 스트림 작업이 이걸 받아 쓴다. */
export type EmitFn = (event: string, payload: Record<string, unknown>) => void;

/**
 * SSE 스트림을 만든다. 작업은 구독이 일어난 뒤에 시작한다.
 *
 * Subject 를 만들고 곧바로 작업을 돌리면, 구독하기 전에 발행된 이벤트가
 * 전부 사라진다. Subject 는 지난 값을 보관하지 않기 때문이다.
 * 특히 시작하자마자 끝나는 경로(설정이 틀려 에러만 내고 끝나는 경우)는
 * 이벤트가 하나도 안 남아서, 화면에서는 아무 일도 일어나지 않은 것처럼 보인다.
 * 에러 메시지조차 못 받으니 왜 안 되는지 알 방법이 없다.
 *
 * defer 로 감싸면 구독하는 순간에 비로소 작업이 시작되므로 첫 이벤트부터 도착한다.
 *
 * @param work 이벤트를 내보내는 작업. emit 으로 발행하고, 끝나면 스트림도 닫힌다.
 */
export function createEventStream(
  work: (emit: EmitFn) => Promise<void>,
): Observable<MessageEvent> {
  return defer(() => {
    const subject = new Subject<MessageEvent>();

    const emit: EmitFn = (event, payload) => {
      subject.next({
        data: JSON.stringify({ event, ...payload }),
      } as MessageEvent);
    };

    // 작업을 한 박자 미뤄서 시작한다.
    // async 함수라도 첫 await 를 만나기 전까지는 그 자리에서 그대로 실행된다.
    // 그래서 곧바로 부르면, 이 함수가 subject 를 돌려주기도 전에 첫 이벤트가
    // 발행되어 사라진다. 특히 시작하자마자 끝나는 경로는 통째로 유실된다.
    queueMicrotask(() => {
      work(emit)
        .then(() => subject.complete())
        .catch((error: unknown) => {
          // 작업이 던지면 스트림을 조용히 닫지 않는다. 조용히 끝내면
          // 화면에서는 정상 완료와 구분되지 않는다.
          const reason = error instanceof Error ? error.message : String(error);
          emit('error', { message: `스트림 작업 실패: ${reason}` });
          subject.complete();
        });
    });

    return subject;
  });
}
