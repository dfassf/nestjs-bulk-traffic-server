import { Module } from '@nestjs/common';
import { QUEUE_PERSISTENCE } from './persistence.interface';
import { FilePersistenceService } from './file-persistence.service';

@Module({
  providers: [
    FilePersistenceService,
    {
      provide: QUEUE_PERSISTENCE,
      useFactory: (filePersistenceService: FilePersistenceService) => {
        const mode = process.env.QUEUE_PERSISTENCE;
        if (mode === 'file') {
          return filePersistenceService;
        }
        return null;
      },
      inject: [FilePersistenceService],
    },
  ],
  exports: [QUEUE_PERSISTENCE],
})
export class PersistenceModule {}
