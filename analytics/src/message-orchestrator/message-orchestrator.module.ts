import { Module } from "@nestjs/common";
import { MessageOrchestratorService } from "./message-orchestrator.service";
import { MessageOrchestratorController } from "./message-orchestrator.controller";
import { TransactionsModule } from "src/transactions/transactions.module";
import { ClientsModule, Transport } from "@nestjs/microservices";

@Module({
  imports: [TransactionsModule,  ClientsModule.register([
    {
      name: 'HERO_SERVICE',
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: 'my-app',
          brokers: ['localhost:29092'],
        },
        consumer: {
          groupId: 'my-consumer'
        }
      }
    },
  ]),],
  controllers: [MessageOrchestratorController],
  providers: [MessageOrchestratorService],
})
export class MessageOrchestratorModule {}
