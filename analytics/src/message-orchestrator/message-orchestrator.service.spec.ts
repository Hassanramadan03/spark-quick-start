import { Test, TestingModule } from '@nestjs/testing';
import { MessageOrchestratorService } from './message-orchestrator.service';

describe('MessageOrchestratorService', () => {
  let service: MessageOrchestratorService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [MessageOrchestratorService],
    }).compile();

    service = module.get<MessageOrchestratorService>(MessageOrchestratorService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
