import { Module } from '@nestjs/common';
import { QUEUE_PERSISTENCE } from './persistence.interface';
import { FilePersistenceService } from './file-persistence.service';

@Module({
  providers: [
    {
      provide: QUEUE_PERSISTENCE,
      useFactory: () => {
        const mode = process.env.QUEUE_PERSISTENCE;
        if (mode === 'file') {
          return new FilePersistenceService();
        }
        return null;
      },
    },
  ],
  exports: [QUEUE_PERSISTENCE],
})
export class PersistenceModule {}
