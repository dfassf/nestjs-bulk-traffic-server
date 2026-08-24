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
