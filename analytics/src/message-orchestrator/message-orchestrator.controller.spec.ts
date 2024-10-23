import { Test, TestingModule } from '@nestjs/testing';
import { MessageOrchestratorController } from './message-orchestrator.controller';
import { MessageOrchestratorService } from './message-orchestrator.service';

describe('MessageOrchestratorController', () => {
  let controller: MessageOrchestratorController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [MessageOrchestratorController],
      providers: [MessageOrchestratorService],
    }).compile();

    controller = module.get<MessageOrchestratorController>(MessageOrchestratorController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
