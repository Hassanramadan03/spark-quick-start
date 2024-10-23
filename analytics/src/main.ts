import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const KAFKA_URI ='0.0.0.0:29092';
    // (process.env.NODE_ENV == 'production' && process.env.KAFKA_URI) ||
    // process.env.KAFKA_BROKER;
    
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        brokers: [KAFKA_URI],
        clientId: 'my-app',
     
      },
      consumer: {
        groupId: 'my-consumer',
        allowAutoTopicCreation: true,
      }
    },
  });

  await app.listen(3001);
  await app.startAllMicroservices();
  console.log(`Application is  running on: ${await app.getUrl()}`);
}
bootstrap();
