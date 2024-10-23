import { Injectable } from '@nestjs/common';

@Injectable()
export class AppService {
  getHello(): any {
    return {
      message: 'welcome in analytics service!',
      version: '1.0.0',
    };
  }
}
