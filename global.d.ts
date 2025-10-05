// Minimal ambient typings for parquetjs-lite used by the project
declare module 'parquetjs-lite' {
  export class ParquetSchema {
    constructor(schema: Record<string, { type: string }>);
  }

  export class ParquetWriter {
    static openStream(
      schema: ParquetSchema,
      output: NodeJS.WritableStream
    ): Promise<ParquetWriter>;

    appendRow(row: Record<string, any>): Promise<void>;
    close(): Promise<void>;
  }

  export class ParquetReader {
    static openBuffer(input: Buffer): Promise<ParquetReader>;
    getCursor(): { next(): Promise<any> };
    close(): Promise<void>;
  }
}