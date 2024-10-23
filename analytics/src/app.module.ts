import { Module } from "@nestjs/common";
import { AppController } from "./app.controller";
import { AppService } from "./app.service";
import { MessageOrchestratorModule } from "./message-orchestrator/message-orchestrator.module";
import { TransactionsModule } from "./transactions/transactions.module";

@Module({
  imports: [MessageOrchestratorModule, TransactionsModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
