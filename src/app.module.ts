import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import {BullModule} from '@nestjs/bullmq';

@Module({
  imports: [
    BullModule.forRootAsync({
      
    }),
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
