import { Controller } from "@nestjs/common";
import { MessageOrchestratorService } from "./message-orchestrator.service";
import {
  Client,
  ClientKafka,
  Ctx,
  KafkaContext,
  MessagePattern,
  Payload,
  Transport,
} from "@nestjs/microservices";
import { Subscription } from "rxjs";
import { tap } from "rxjs/operators";
import {
  ON_DELETED_TRANSACTION_TOPIC,
  TRANSACTION_TOPIC,
  TRANSACTION_TOPIC_RESULT,
} from "src/constants/constants";
import { TransactionsService } from "src/transactions/transactions.service";
@Controller()
export class MessageOrchestratorController {
  @Client({
    transport: Transport.KAFKA,
    options: {
      client: {
        brokers: ["localhost:29092"],
        clientId: "my-app",
      } ,
      consumer: {
        groupId: "my-consumer",
        allowAutoTopicCreation: true,
      },
      
    },
  })

  client: ClientKafka;
  subscriptions: Subscription[] = [];
  constructor(
    private readonly messageOrchestratorService: MessageOrchestratorService,
    private readonly transctionService: TransactionsService
  ) {}
 async onModuleInit() {
    this.client.subscribeToResponseOf(TRANSACTION_TOPIC_RESULT);
    await this.client.connect();

  }
  @MessagePattern(TRANSACTION_TOPIC_RESULT)
  async consumeKafkaMessages(
    @Payload() message: any,
    @Ctx() _context: KafkaContext
  ): Promise<any> {
    // const heartbeat = _context.getHeartbeat();
    this.transctionService.onCreateTransaction(message);
    // heartbeat()
  }
  @MessagePattern(ON_DELETED_TRANSACTION_TOPIC)
  async onDeletedReservation(
    @Payload() message: any,
    @Ctx() _context: KafkaContext
  ): Promise<any> {
    this.transctionService.onDeleteTransaction(message);
  }
  onDestroy() {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
  }
}
