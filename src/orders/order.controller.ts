import {
  Body,
  Controller,
  Delete,
  Get,
  NotFoundException,
  Param,
  Post,
  Query,
} from '@nestjs/common';
import { OrderService } from './order.service';
import { OrderItem } from './order-events';

const MAX_BULK_COUNT = 10000;
const MAX_EVENT_LIMIT = 1000;
// 주문 하나당 이벤트 4건이 나가므로 건수를 낮게 잡는다.
const MAX_LIFECYCLE_COUNT = 500;

@Controller('orders')
export class OrderController {
  constructor(private readonly orderService: OrderService) {}

  @Post()
  async create(@Body() body: { userId?: string; items?: OrderItem[] }) {
    const { order, dispatch } = await this.orderService.createOrder(body ?? {});
    return {
      orderId: order.orderId,
      status: order.status,
      amount: order.amount,
      dispatch,
    };
  }

  /** 대량 생성. 부하를 걸어 Lag·파티션 분배를 볼 때 쓴다. */
  @Post('bulk')
  async createBulk(@Body() body: { count?: number; delayMs?: number }) {
    const count = Math.min(Math.max(body?.count ?? 100, 1), MAX_BULK_COUNT);
    const delayMs = Math.max(body?.delayMs ?? 0, 0);
    return this.orderService.createBulk(count, delayMs);
  }

  /**
   * 주문을 만들고 흐름 전체(생성 → 재고 → 결제 → 배송)를 내보낸다.
   *
   * 키를 쓰면 같은 주문의 단계가 같은 파티션으로 모이고, 키를 빼면 흩어진다.
   * 그 차이를 보는 실험(키가 순서를 지배한다)에서 쓴다.
   */
  @Post('lifecycle')
  async createLifecycle(@Body() body: { count?: number }) {
    const count = Math.min(Math.max(body?.count ?? 10, 1), MAX_LIFECYCLE_COUNT);
    return this.orderService.createWithLifecycle(count);
  }

  @Get('stats')
  async stats() {
    return this.orderService.getStats();
  }

  /** 이벤트 소비 기록. 중복·순서를 확인하는 자리. */
  @Get('events')
  async events(
    @Query('orderId') orderId?: string,
    @Query('limit') limit?: string,
  ) {
    const parsed = limit ? Number(limit) : undefined;
    const safeLimit =
      parsed !== undefined && Number.isFinite(parsed) && parsed > 0
        ? Math.min(parsed, MAX_EVENT_LIMIT)
        : undefined;

    return {
      events: await this.orderService.findEvents(orderId, safeLimit),
    };
  }

  @Get('duplicates')
  async duplicates() {
    const duplicates = await this.orderService.findDuplicates();
    return {
      groups: duplicates.length,
      extra: duplicates.reduce((sum, d) => sum + (d.count - 1), 0),
      duplicates,
    };
  }

  /**
   * 컨슈머 그룹별 소비 건수.
   *
   * 여러 그룹이 같은 토픽을 구독할 때, 나눠 가진 것인지 각자 전량 받은 것인지는
   * 전체 건수로는 구분되지 않는다. 그룹으로 갈라야 보인다.
   */
  @Get('groups')
  async groups() {
    const groups = await this.orderService.countByGroup();
    return {
      groups: groups.length,
      detail: groups,
    };
  }

  @Delete()
  async reset() {
    await this.orderService.reset();
    return { ok: true };
  }

  // 경로 파라미터 라우트를 마지막에 둔다. 위에 두면 /orders/stats 가 여기로 잡힌다.
  @Get(':orderId')
  async findOne(@Param('orderId') orderId: string) {
    const order = await this.orderService.findOrder(orderId);
    if (!order) {
      throw new NotFoundException(`주문을 찾을 수 없습니다: ${orderId}`);
    }
    return order;
  }
}
