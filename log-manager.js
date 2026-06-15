const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { EventEmitter } = require('events');

const DEFAULT_OPTIONS = {
  splitStrategy: 'daily',
  maxFileSize: 50 * 1024 * 1024,
  maxRecordsPerFile: 10000,
  logDir: '',
  baseName: 'performance_log',
  flushInterval: 5000,
  maxBufferedRecords: 1000,
  encoding: 'utf-8',
  enableCompression: true,
  compressionThreshold: 100 * 1024 * 1024,
  compressionLevel: 6,
  autoCompressCheckInterval: 60000
};

class LogManager extends EventEmitter {
  constructor(options = {}) {
    super();
    this.options = { ...DEFAULT_OPTIONS, ...options };
    this.isLogging = false;
    this.currentFile = '';
    this.currentFileSize = 0;
    this.currentRecordCount = 0;
    this.currentFileStartDate = null;
    this.buffer = [];
    this.flushTimer = null;
    this.fileIndex = [];
    this.writeStream = null;
    this.isFlushing = false;
    this.isCompressing = new Set();
    this.compressCheckTimer = null;
  }

  async start(logDir) {
    if (this.isLogging) return;

    if (logDir) {
      this.options.logDir = logDir;
    }

    if (!this.options.logDir) {
      throw new Error('日志目录未指定');
    }

    if (!fs.existsSync(this.options.logDir)) {
      fs.mkdirSync(this.options.logDir, { recursive: true });
    }

    await this._loadFileIndex();
    await this._createNewFile();
    this.isLogging = true;

    this.flushTimer = setInterval(() => {
      this._flush();
    }, this.options.flushInterval);

    if (this.options.enableCompression) {
      this.compressCheckTimer = setInterval(() => {
        this._checkAndCompressOldFiles();
      }, this.options.autoCompressCheckInterval);
    }

    this.emit('started', { file: this.currentFile });
  }

  async stop() {
    if (!this.isLogging) return;

    this.isLogging = false;

    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }

    if (this.compressCheckTimer) {
      clearInterval(this.compressCheckTimer);
      this.compressCheckTimer = null;
    }

    await this._flush(true);
    await this._closeWriteStream();
    await this._saveFileIndex();

    this.emit('stopped', { 
      file: this.currentFile, 
      totalRecords: this.getTotalRecordCount() 
    });
  }

  addRecord(record) {
    if (!this.isLogging) return false;

    const recordWithMeta = {
      ...record,
      _seq: Date.now() + Math.random()
    };

    this.buffer.push(recordWithMeta);

    if (this.buffer.length >= this.options.maxBufferedRecords) {
      setImmediate(() => this._flush());
    }

    this.emit('record-added', { count: this.buffer.length });
    return true;
  }

  async _createNewFile() {
    await this._closeWriteStream();

    const now = new Date();
    const dateStr = now.toISOString().slice(0, 10);
    const timeStr = now.toISOString().slice(11, 19).replace(/:/g, '-');
    
    let baseFileName;
    if (this.options.splitStrategy === 'size') {
      baseFileName = `${this.options.baseName}_${dateStr}_${timeStr}`;
    } else {
      baseFileName = `${this.options.baseName}_${dateStr}`;
    }

    let fileIndex = 0;
    let fileName, filePath;
    do {
      const suffix = fileIndex > 0 ? `_${fileIndex}` : '';
      fileName = `${baseFileName}${suffix}.jsonl`;
      filePath = path.join(this.options.logDir, fileName);
      fileIndex++;
    } while (fs.existsSync(filePath) || fs.existsSync(filePath + '.gz'));

    this.currentFile = filePath;

    this.writeStream = fs.createWriteStream(this.currentFile, {
      flags: 'a',
      encoding: this.options.encoding
    });

    this.currentFileSize = 0;
    this.currentRecordCount = 0;
    this.currentFileStartDate = now;

    const indexEntry = {
      file: fileName,
      startTime: now.toISOString(),
      endTime: now.toISOString(),
      recordCount: 0,
      size: 0,
      compressed: false,
      originalSize: 0,
      compressedSize: 0,
      compressedAt: null
    };
    this.fileIndex.push(indexEntry);

    this.emit('file-created', { file: this.currentFile });
  }

  async _checkAndSplit(pendingRecords = []) {
    let needSplit = false;

    if (this.currentRecordCount > 0) {
      if (this.options.splitStrategy === 'daily') {
        const today = new Date().toISOString().slice(0, 10);
        const fileDay = this.currentFileStartDate.toISOString().slice(0, 10);
        if (today !== fileDay) {
          needSplit = true;
        }
      }

      const pendingSize = pendingRecords.reduce((sum, r) => {
        return sum + Buffer.byteLength(JSON.stringify(r) + '\n', this.options.encoding);
      }, 0);

      const projectedSize = this.currentFileSize + pendingSize;
      const projectedCount = this.currentRecordCount + pendingRecords.length;

      if (projectedSize >= this.options.maxFileSize || projectedCount >= this.options.maxRecordsPerFile) {
        needSplit = true;
      }
    }

    if (needSplit) {
      await this._saveFileIndex();
      
      const justClosedFile = this.currentFile;
      const justClosedIndex = this.fileIndex.length - 1;
      
      await this._createNewFile();
      
      if (this.options.enableCompression && justClosedIndex >= 0) {
        setImmediate(() => {
          this._maybeCompressFile(justClosedFile, justClosedIndex);
        });
      }
    }
  }

  async _flush(force = false) {
    if (this.isFlushing || (this.buffer.length === 0 && !force)) return;
    
    this.isFlushing = true;
    const recordsToWrite = [...this.buffer];
    this.buffer = [];

    try {
      await this._checkAndSplit(recordsToWrite);

      const lines = recordsToWrite.map(r => JSON.stringify(r));
      const dataToWrite = lines.join('\n') + '\n';
      const byteLength = Buffer.byteLength(dataToWrite, this.options.encoding);

      await new Promise((resolve, reject) => {
        if (!this.writeStream) {
          reject(new Error('Write stream not available'));
          return;
        }
        this.writeStream.write(dataToWrite, this.options.encoding, (err) => {
          if (err) reject(err);
          else resolve();
        });
      });

      this.currentFileSize += byteLength;
      this.currentRecordCount += recordsToWrite.length;

      const lastIndex = this.fileIndex.length - 1;
      if (lastIndex >= 0) {
        this.fileIndex[lastIndex].endTime = recordsToWrite[recordsToWrite.length - 1]?.timestamp || new Date().toISOString();
        this.fileIndex[lastIndex].recordCount = this.currentRecordCount;
        this.fileIndex[lastIndex].size = this.currentFileSize;
      }

      this.emit('flushed', { 
        count: recordsToWrite.length, 
        file: this.currentFile,
        totalInFile: this.currentRecordCount
      });

    } catch (err) {
      this.buffer.unshift(...recordsToWrite);
      this.emit('error', { type: 'flush', error: err.message });
    } finally {
      this.isFlushing = false;
    }
  }

  async _closeWriteStream() {
    if (this.writeStream) {
      await new Promise(resolve => {
        this.writeStream.end(() => resolve());
      });
      this.writeStream = null;
    }
  }

  async _loadFileIndex() {
    const indexPath = path.join(this.options.logDir, '.log_index.json');
    this.fileIndex = [];

    if (fs.existsSync(indexPath)) {
      try {
        const content = fs.readFileSync(indexPath, this.options.encoding);
        this.fileIndex = JSON.parse(content);
      } catch (err) {
        this.fileIndex = [];
      }
    }

    if (this.fileIndex.length === 0) {
      await this._rebuildIndex();
    } else {
      for (const entry of this.fileIndex) {
        if (entry.compressed === undefined) entry.compressed = false;
        if (entry.originalSize === undefined) entry.originalSize = entry.size || 0;
        if (entry.compressedSize === undefined) entry.compressedSize = 0;
        if (entry.compressedAt === undefined) entry.compressedAt = null;
      }
    }
  }

  async _rebuildIndex() {
    const allFiles = fs.readdirSync(this.options.logDir).sort();
    const jsonlFiles = allFiles.filter(f => 
      f.startsWith(this.options.baseName) && 
      (f.endsWith('.jsonl') || f.endsWith('.jsonl.gz'))
    );

    this.fileIndex = [];

    for (const file of jsonlFiles) {
      const filePath = path.join(this.options.logDir, file);
      const stats = fs.statSync(filePath);
      const isCompressed = file.endsWith('.gz');
      const displayName = isCompressed ? file.slice(0, -3) : file;
      
      let firstRecord = null;
      let lastRecord = null;
      let count = 0;

      try {
        const stream = await this._createReadStream(filePath);
        let buffer = '';
        
        for await (const chunk of stream) {
          buffer += chunk;
          const lines = buffer.split('\n');
          buffer = lines.pop();
          
          for (const line of lines) {
            if (line.trim()) {
              count++;
              try {
                const record = JSON.parse(line);
                if (!firstRecord) firstRecord = record;
                lastRecord = record;
              } catch {}
            }
          }
        }
      } catch {}

      if (count > 0) {
        const fileEntry = {
          file: file,
          startTime: firstRecord?.timestamp || stats.birthtime.toISOString(),
          endTime: lastRecord?.timestamp || stats.mtime.toISOString(),
          recordCount: count,
          size: stats.size,
          compressed: isCompressed,
          originalSize: isCompressed ? 0 : stats.size,
          compressedSize: isCompressed ? stats.size : 0,
          compressedAt: isCompressed ? stats.mtime.toISOString() : null
        };
        this.fileIndex.push(fileEntry);
      } else {
        try {
          fs.unlinkSync(filePath);
        } catch {}
      }
    }

    await this._saveFileIndex();
  }

  async _saveFileIndex() {
    const indexPath = path.join(this.options.logDir, '.log_index.json');
    try {
      fs.writeFileSync(indexPath, JSON.stringify(this.fileIndex, null, 2), this.options.encoding);
    } catch (err) {
      this.emit('error', { type: 'save-index', error: err.message });
    }
  }

  async _createReadStream(filePath) {
    const isCompressed = filePath.endsWith('.gz');
    let stream = fs.createReadStream(filePath, { 
      encoding: isCompressed ? undefined : this.options.encoding,
      highWaterMark: 64 * 1024
    });
    
    if (isCompressed) {
      const gunzip = zlib.createGunzip();
      stream = stream.pipe(gunzip);
      stream.setEncoding(this.options.encoding);
    }
    
    return stream;
  }

  async _checkAndCompressOldFiles() {
    if (!this.options.enableCompression) return;

    const currentBaseName = path.basename(this.currentFile);

    for (let i = 0; i < this.fileIndex.length; i++) {
      const entry = this.fileIndex[i];
      if (entry.compressed) continue;
      if (entry.file === currentBaseName) continue;
      if (this.isCompressing.has(entry.file)) continue;

      const filePath = path.join(this.options.logDir, entry.file);
      if (!fs.existsSync(filePath)) continue;

      const stats = fs.statSync(filePath);
      if (stats.size >= this.options.compressionThreshold) {
        this._maybeCompressFile(filePath, i);
      }
    }
  }

  _maybeCompressFile(filePath, indexPos) {
    if (!this.options.enableCompression) return;
    
    const entry = this.fileIndex[indexPos];
    if (!entry || entry.compressed) return;
    if (this.isCompressing.has(entry.file)) return;

    const actualSize = fs.existsSync(filePath) ? fs.statSync(filePath).size : 0;
    if (actualSize < this.options.compressionThreshold) return;

    this.isCompressing.add(entry.file);
    
    this._compressFileInternal(filePath, indexPos)
      .then(() => {
        this.isCompressing.delete(entry.file);
      })
      .catch((err) => {
        this.isCompressing.delete(entry.file);
        this.emit('error', { type: 'compression', file: entry.file, error: err.message });
      });
  }

  async _compressFileInternal(filePath, indexPos) {
    const entry = this.fileIndex[indexPos];
    const gzPath = filePath + '.gz';
    const tmpPath = gzPath + '.tmp';

    this.emit('compression-started', { file: entry.file });

    const originalSize = fs.statSync(filePath).size;

    await new Promise((resolve, reject) => {
      const readStream = fs.createReadStream(filePath);
      const gzip = zlib.createGzip({ level: this.options.compressionLevel });
      const writeStream = fs.createWriteStream(tmpPath);

      readStream.on('error', reject);
      gzip.on('error', reject);
      writeStream.on('error', reject);
      writeStream.on('finish', resolve);

      readStream.pipe(gzip).pipe(writeStream);
    });

    const compressedSize = fs.statSync(tmpPath).size;

    fs.renameSync(tmpPath, gzPath);
    fs.unlinkSync(filePath);

    entry.file = entry.file + '.gz';
    entry.compressed = true;
    entry.originalSize = originalSize;
    entry.compressedSize = compressedSize;
    entry.size = compressedSize;
    entry.compressedAt = new Date().toISOString();

    await this._saveFileIndex();

    this.emit('compression-finished', {
      file: entry.file,
      originalSize,
      compressedSize,
      ratio: originalSize > 0 ? (1 - compressedSize / originalSize) : 0
    });
  }

  async compressFile(fileName) {
    let indexPos = this.fileIndex.findIndex(e => e.file === fileName);
    if (indexPos < 0) {
      const gzName = fileName.endsWith('.gz') ? fileName : fileName + '.gz';
      indexPos = this.fileIndex.findIndex(e => e.file === gzName);
      if (indexPos >= 0 && this.fileIndex[indexPos].compressed) {
        return { alreadyCompressed: true, file: gzName };
      }
      throw new Error('文件不存在: ' + fileName);
    }

    const entry = this.fileIndex[indexPos];
    if (entry.compressed) {
      return { alreadyCompressed: true, file: entry.file };
    }

    const currentBaseName = path.basename(this.currentFile);
    if (entry.file === currentBaseName) {
      await this._flush(true);
      await this._saveFileIndex();
      await this._createNewFile();
      indexPos = this.fileIndex.findIndex(e => e.file === fileName);
    }

    if (indexPos < 0) throw new Error('文件不存在: ' + fileName);

    const filePath = path.join(this.options.logDir, this.fileIndex[indexPos].file);
    if (!fs.existsSync(filePath)) throw new Error('文件不存在: ' + fileName);

    this.isCompressing.add(this.fileIndex[indexPos].file);
    try {
      await this._compressFileInternal(filePath, indexPos);
      return { 
        success: true, 
        file: this.fileIndex[indexPos].file,
        originalSize: this.fileIndex[indexPos].originalSize,
        compressedSize: this.fileIndex[indexPos].compressedSize
      };
    } finally {
      this.isCompressing.delete(fileName);
    }
  }

  async decompressFile(fileName) {
    let indexPos = this.fileIndex.findIndex(e => e.file === fileName);
    if (indexPos < 0) {
      const baseName = fileName.endsWith('.gz') ? fileName.slice(0, -3) : fileName;
      const gzName = baseName + '.gz';
      indexPos = this.fileIndex.findIndex(e => e.file === gzName);
      if (indexPos < 0) {
        throw new Error('文件不存在: ' + fileName);
      }
      fileName = gzName;
    }

    const entry = this.fileIndex[indexPos];
    if (!entry.compressed) {
      return { alreadyDecompressed: true, file: entry.file };
    }

    const gzPath = path.join(this.options.logDir, fileName);
    const basePath = gzPath.slice(0, -3);
    const tmpPath = basePath + '.tmp';

    if (!fs.existsSync(gzPath)) throw new Error('压缩文件不存在: ' + fileName);

    this.emit('decompression-started', { file: fileName });

    await new Promise((resolve, reject) => {
      const readStream = fs.createReadStream(gzPath);
      const gunzip = zlib.createGunzip();
      const writeStream = fs.createWriteStream(tmpPath);

      readStream.on('error', reject);
      gunzip.on('error', reject);
      writeStream.on('error', reject);
      writeStream.on('finish', resolve);

      readStream.pipe(gunzip).pipe(writeStream);
    });

    fs.renameSync(tmpPath, basePath);
    fs.unlinkSync(gzPath);

    const decompressedSize = fs.statSync(basePath).size;

    entry.file = path.basename(basePath);
    entry.compressed = false;
    entry.originalSize = decompressedSize;
    entry.compressedSize = 0;
    entry.size = decompressedSize;
    entry.compressedAt = null;

    await this._saveFileIndex();

    this.emit('decompression-finished', {
      file: entry.file,
      size: decompressedSize
    });

    return { success: true, file: entry.file, size: decompressedSize };
  }

  async compressAllOldFiles() {
    if (!this.options.enableCompression) return { skipped: true };

    const currentBaseName = path.basename(this.currentFile);
    const results = [];

    for (let i = 0; i < this.fileIndex.length; i++) {
      const entry = this.fileIndex[i];
      if (entry.compressed) continue;
      if (entry.file === currentBaseName) continue;

      try {
        const filePath = path.join(this.options.logDir, entry.file);
        if (!fs.existsSync(filePath)) continue;

        this.isCompressing.add(entry.file);
        await this._compressFileInternal(filePath, i);
        this.isCompressing.delete(entry.file);
        results.push({ 
          file: this.fileIndex[i]?.file || entry.file, 
          success: true 
        });
      } catch (err) {
        this.isCompressing.delete(entry.file);
        results.push({ file: entry.file, success: false, error: err.message });
      }
    }

    return { 
      total: results.length, 
      success: results.filter(r => r.success).length,
      failed: results.filter(r => !r.success).length,
      details: results 
    };
  }

  async queryRecords(options = {}) {
    const { 
      startTime, 
      endTime, 
      limit = 1000, 
      offset = 0,
      filters = {}
    } = options;

    const matchingFiles = this._findMatchingFiles(startTime, endTime);
    const results = [];
    let skipped = 0;

    for (const indexEntry of matchingFiles) {
      if (results.length >= limit) break;

      const filePath = path.join(this.options.logDir, indexEntry.file);
      const records = await this._readFileRecords(filePath, {
        startTime,
        endTime,
        filters,
        limit: limit - results.length,
        skip: Math.max(0, offset - skipped)
      });

      skipped += records.skipped;
      results.push(...records.data);
    }

    return {
      data: results,
      total: this._estimateTotalCount(matchingFiles, startTime, endTime),
      hasMore: results.length >= limit
    };
  }

  _findMatchingFiles(startTime, endTime) {
    return this.fileIndex.filter(entry => {
      if (startTime && entry.endTime < startTime) return false;
      if (endTime && entry.startTime > endTime) return false;
      return true;
    });
  }

  async _readFileRecords(filePath, options) {
    const { startTime, endTime, filters, limit, skip } = options;
    const results = [];
    let skipped = 0;
    let readCount = 0;

    const stream = await this._createReadStream(filePath);
    let buffer = '';

    for await (const chunk of stream) {
      buffer += chunk;
      const lines = buffer.split('\n');
      buffer = lines.pop();

      for (const line of lines) {
        if (!line.trim()) continue;
        
        if (skip > 0 && skipped < skip) {
          skipped++;
          continue;
        }

        if (results.length >= limit) break;

        try {
          const record = JSON.parse(line);
          
          if (startTime && record.timestamp < startTime) continue;
          if (endTime && record.timestamp > endTime) continue;
          
          let match = true;
          for (const [key, value] of Object.entries(filters)) {
            const keys = key.split('.');
            let r = record;
            for (const k of keys) {
              r = r?.[k];
            }
            if (r !== value) {
              match = false;
              break;
            }
          }
          
          if (match) {
            results.push(record);
          }
        } catch {}
      }

      if (results.length >= limit) break;
    }

    return { data: results, skipped, readCount };
  }

  _estimateTotalCount(files, startTime, endTime) {
    return files.reduce((sum, f) => sum + f.recordCount, 0);
  }

  async exportReport(options) {
    const { 
      format = 'csv', 
      outputPath, 
      startTime, 
      endTime,
      includeProcesses = true,
      includeSummary = true,
      fields = null,
      onProgress = null
    } = options;

    const matchingFiles = this._findMatchingFiles(startTime, endTime);
    const totalEstimated = this._estimateTotalCount(matchingFiles, startTime, endTime);
    let totalExported = 0;
    let processedCount = 0;
    let lastProgressEmit = 0;

    const summary = {
      cpu: { sum: 0, count: 0, avg: 0, max: -Infinity, min: Infinity },
      memory: { sum: 0, count: 0, avg: 0, max: -Infinity, min: Infinity },
      disk: { sum: 0, count: 0, avg: 0, max: -Infinity, min: Infinity }
    };

    const emitProgress = () => {
      if (onProgress && totalEstimated > 0) {
        const now = Date.now();
        if (now - lastProgressEmit >= 200) {
          lastProgressEmit = now;
          onProgress({
            exported: totalExported,
            total: totalEstimated,
            percent: Math.min(100, Math.round((totalExported / totalEstimated) * 100)),
            currentFile: matchingFiles[Math.min(matchingFiles.length - 1, 
              Math.floor((processedCount / Math.max(1, totalEstimated)) * matchingFiles.length))]?.file || ''
          });
        }
      }
    };

    const writeStream = fs.createWriteStream(outputPath, { encoding: this.options.encoding });
    
    const writeAsync = (data) => {
      return new Promise((resolve, reject) => {
        if (writeStream.write(data, this.options.encoding)) {
          resolve();
        } else {
          writeStream.once('drain', resolve);
        }
      });
    };

    try {
      let isFirstRecord = true;

      if (format === 'csv') {
        const headers = [
          '时间',
          'CPU使用率(%)',
          '内存使用率(%)',
          '磁盘使用率(%)',
          '网络上传(MB/s)',
          '网络下载(MB/s)',
          '内存已用(GB)',
          '内存总量(GB)',
          '磁盘已用(GB)',
          '磁盘总量(GB)'
        ];
        if (includeProcesses) {
          headers.push('Top进程名称', 'Top进程CPU(%)', 'Top进程内存(%)');
        }
        await writeAsync(headers.join(',') + '\n');
      } else if (format === 'jsonl') {
      } else {
        await writeAsync('{\n');
        await writeAsync(`  "generatedAt": "${new Date().toISOString()}",\n`);
        await writeAsync(`  "startTime": ${startTime ? `"${startTime}"` : 'null'},\n`);
        await writeAsync(`  "endTime": ${endTime ? `"${endTime}"` : 'null'},\n`);
        await writeAsync('  "data": [\n');
      }

      for (const indexEntry of matchingFiles) {
        const filePath = path.join(this.options.logDir, indexEntry.file);
        const stream = await this._createReadStream(filePath);
        let buffer = '';

        for await (const chunk of stream) {
          buffer += chunk;
          const lines = buffer.split('\n');
          buffer = lines.pop();

          for (const line of lines) {
            if (!line.trim()) continue;
            
            let record;
            try {
              record = JSON.parse(line);
            } catch {
              continue;
            }
            
            if (startTime && record.timestamp < startTime) continue;
            if (endTime && record.timestamp > endTime) continue;

            summary.cpu.sum += record.cpu.usage;
            summary.cpu.count++;
            summary.cpu.max = Math.max(summary.cpu.max, record.cpu.usage);
            summary.cpu.min = Math.min(summary.cpu.min, record.cpu.usage);
            
            summary.memory.sum += record.memory.usage;
            summary.memory.count++;
            summary.memory.max = Math.max(summary.memory.max, record.memory.usage);
            summary.memory.min = Math.min(summary.memory.min, record.memory.usage);
            
            summary.disk.sum += record.disk.usage;
            summary.disk.count++;
            summary.disk.max = Math.max(summary.disk.max, record.disk.usage);
            summary.disk.min = Math.min(summary.disk.min, record.disk.usage);

            if (format === 'csv') {
              const row = [
                record.timestamp,
                record.cpu.usage,
                record.memory.usage,
                record.disk.usage,
                record.network.upMB,
                record.network.downMB,
                (record.memory.used / 1024 / 1024 / 1024).toFixed(2),
                (record.memory.total / 1024 / 1024 / 1024).toFixed(2),
                (record.disk.used / 1024 / 1024 / 1024).toFixed(2),
                (record.disk.total / 1024 / 1024 / 1024).toFixed(2)
              ];
              if (includeProcesses && record.topProcesses && record.topProcesses[0]) {
                row.push(
                  `"${record.topProcesses[0].name.replace(/"/g, '""')}"`,
                  record.topProcesses[0].cpu,
                  record.topProcesses[0].mem
                );
              }
              await writeAsync(row.join(',') + '\n');
            } else if (format === 'jsonl') {
              await writeAsync(JSON.stringify(record) + '\n');
            } else {
              if (!isFirstRecord) await writeAsync(',\n');
              await writeAsync(`    ${JSON.stringify(record)}`);
              isFirstRecord = false;
            }

            totalExported++;
            processedCount++;
            emitProgress();
          }
        }
      }

      if (format === 'csv') {
        if (includeSummary) {
          const calcAvg = (s) => s.count > 0 ? parseFloat((s.sum / s.count).toFixed(2)) : 0;
          await writeAsync('\n');
          await writeAsync('# 统计摘要\n');
          await writeAsync(`# CPU - 平均:${calcAvg(summary.cpu)}%, 最高:${summary.cpu.max.toFixed(2)}%, 最低:${summary.cpu.min.toFixed(2)}%\n`);
          await writeAsync(`# 内存 - 平均:${calcAvg(summary.memory)}%, 最高:${summary.memory.max.toFixed(2)}%, 最低:${summary.memory.min.toFixed(2)}%\n`);
          await writeAsync(`# 磁盘 - 平均:${calcAvg(summary.disk)}%, 最高:${summary.disk.max.toFixed(2)}%, 最低:${summary.disk.min.toFixed(2)}%\n`);
          await writeAsync(`# 总记录数: ${totalExported}\n`);
        }
      } else if (format === 'jsonl') {
      } else {
        const calcAvg = (s) => s.count > 0 ? parseFloat((s.sum / s.count).toFixed(2)) : 0;
        
        await writeAsync('\n  ],\n');
        
        if (includeSummary) {
          const finalSummary = {
            cpu: { 
              avg: calcAvg(summary.cpu), 
              max: parseFloat(summary.cpu.max.toFixed(2)), 
              min: parseFloat(summary.cpu.min.toFixed(2)) 
            },
            memory: { 
              avg: calcAvg(summary.memory), 
              max: parseFloat(summary.memory.max.toFixed(2)), 
              min: parseFloat(summary.memory.min.toFixed(2)) 
            },
            disk: { 
              avg: calcAvg(summary.disk), 
              max: parseFloat(summary.disk.max.toFixed(2)), 
              min: parseFloat(summary.disk.min.toFixed(2)) 
            }
          };
          await writeAsync(`  "summary": ${JSON.stringify(finalSummary, null, 2)},\n`);
        }
        
        await writeAsync(`  "recordCount": ${totalExported}\n`);
        await writeAsync('}\n');
      }

      await new Promise(resolve => writeStream.end(resolve));

      if (onProgress) {
        onProgress({
          exported: totalExported,
          total: totalExported,
          percent: 100,
          done: true
        });
      }

      const finalSummary = {
        cpu: { 
          avg: summary.cpu.count > 0 ? parseFloat((summary.cpu.sum / summary.cpu.count).toFixed(2)) : 0,
          max: parseFloat(summary.cpu.max.toFixed(2)),
          min: parseFloat(summary.cpu.min.toFixed(2))
        },
        memory: {
          avg: summary.memory.count > 0 ? parseFloat((summary.memory.sum / summary.memory.count).toFixed(2)) : 0,
          max: parseFloat(summary.memory.max.toFixed(2)),
          min: parseFloat(summary.memory.min.toFixed(2))
        },
        disk: {
          avg: summary.disk.count > 0 ? parseFloat((summary.disk.sum / summary.disk.count).toFixed(2)) : 0,
          max: parseFloat(summary.disk.max.toFixed(2)),
          min: parseFloat(summary.disk.min.toFixed(2))
        }
      };

      return { totalExported, outputPath, summary: finalSummary };
    } catch (err) {
      writeStream.destroy();
      throw err;
    }
  }

  getFileList() {
    return this.fileIndex.map(entry => ({
      ...entry,
      compressionRatio: entry.compressed && entry.originalSize > 0
        ? parseFloat(((1 - entry.compressedSize / entry.originalSize) * 100).toFixed(1))
        : 0
    }));
  }

  getTotalRecordCount() {
    return this.fileIndex.reduce((sum, f) => sum + f.recordCount, 0) + this.buffer.length;
  }

  getCurrentFile() {
    return this.currentFile;
  }

  getCurrentRecordCount() {
    return this.currentRecordCount + this.buffer.length;
  }

  async deleteOldFiles(daysToKeep) {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);
    const cutoffStr = cutoffDate.toISOString();

    const toDelete = this.fileIndex.filter(f => f.endTime < cutoffStr);
    let deletedCount = 0;

    for (const entry of toDelete) {
      const filePath = path.join(this.options.logDir, entry.file);
      try {
        fs.unlinkSync(filePath);
        deletedCount++;
      } catch {}
    }

    this.fileIndex = this.fileIndex.filter(f => f.endTime >= cutoffStr);
    await this._saveFileIndex();

    this.emit('files-deleted', { count: deletedCount });
    return deletedCount;
  }

  async mergeFiles(options = {}) {
    const { outputPath, startTime, endTime } = options;
    const result = await this.exportReport({
      format: 'json',
      outputPath,
      startTime,
      endTime
    });
    return result;
  }

  getCompressionStats() {
    const compressedFiles = this.fileIndex.filter(f => f.compressed);
    const totalOriginalSize = compressedFiles.reduce((sum, f) => sum + (f.originalSize || 0), 0);
    const totalCompressedSize = compressedFiles.reduce((sum, f) => sum + (f.compressedSize || f.size || 0), 0);

    return {
      totalFiles: this.fileIndex.length,
      compressedCount: compressedFiles.length,
      uncompressedCount: this.fileIndex.length - compressedFiles.length,
      totalOriginalSize,
      totalCompressedSize,
      savedBytes: totalOriginalSize - totalCompressedSize,
      averageRatio: totalOriginalSize > 0 
        ? parseFloat(((1 - totalCompressedSize / totalOriginalSize) * 100).toFixed(1))
        : 0
    };
  }
}

module.exports = LogManager;
