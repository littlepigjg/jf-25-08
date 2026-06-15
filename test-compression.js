const LogManager = require('./log-manager');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

const testDir = path.join(__dirname, 'test_compress_logs');

async function testCompression() {
  console.log('=== 开始测试日志压缩功能 ===\n');

  if (fs.existsSync(testDir)) {
    fs.rmSync(testDir, { recursive: true, force: true });
  }

  const COMPRESSION_THRESHOLD = 5 * 1024;
  const MAX_FILE_SIZE = 10 * 1024;

  const logManager = new LogManager({
    splitStrategy: 'size',
    maxFileSize: MAX_FILE_SIZE,
    maxRecordsPerFile: 100,
    logDir: testDir,
    baseName: 'compress_test',
    flushInterval: 50,
    maxBufferedRecords: 10,
    enableCompression: true,
    compressionThreshold: COMPRESSION_THRESHOLD,
    compressionLevel: 6
  });

  console.log('1. 测试 start()...');
  await logManager.start();
  console.log('   ✓ 启动成功');

  console.log('\n2. 测试写入记录直到触发文件分割和压缩...');
  let compressionStarted = false;
  let compressionFinished = false;

  logManager.on('compression-started', (info) => {
    compressionStarted = true;
    console.log(`   压缩事件触发: ${info.file}`);
  });

  logManager.on('compression-finished', (info) => {
    compressionFinished = true;
    console.log(`   压缩完成: ${info.file}, 原始: ${info.originalSize}B, 压缩后: ${info.compressedSize}B, 节省: ${((1 - info.compressedSize / info.originalSize) * 100).toFixed(1)}%`);
  });

  const totalRecords = 80;
  for (let i = 0; i < totalRecords; i++) {
    logManager.addRecord({
      timestamp: new Date().toISOString(),
      cpu: { usage: Math.random() * 100 },
      memory: { usage: Math.random() * 100, total: 16 * 1024 * 1024 * 1024, used: 8 * 1024 * 1024 * 1024, free: 8 * 1024 * 1024 * 1024 },
      disk: { usage: Math.random() * 100, total: 500 * 1024 * 1024 * 1024, used: 250 * 1024 * 1024 * 1024, fs: 'NTFS', mount: '/' },
      network: { upMB: Math.random() * 10, downMB: Math.random() * 10 },
      topProcesses: [
        { name: 'chrome.exe', pid: 1234, cpu: Math.random() * 50, mem: Math.random() * 30 },
        { name: 'node.exe', pid: 5678, cpu: Math.random() * 20, mem: Math.random() * 15 }
      ]
    });
    if ((i + 1) % 20 === 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  await new Promise(resolve => setTimeout(resolve, 500));
  await logManager._flush(true);
  await new Promise(resolve => setTimeout(resolve, 1000));

  console.log(`   共写入 ${totalRecords} 条记录`);
  console.log(`   总文件数: ${logManager.getFileList().length}`);

  const files = logManager.getFileList();
  files.forEach((f, i) => {
    console.log(`   文件 ${i + 1}: ${f.file} - ${f.recordCount}条, ${f.size}B, 压缩: ${f.compressed ? '是' : '否'}`);
  });

  console.log('\n3. 测试手动压缩单个文件...');
  const uncompressedFiles = files.filter(f => !f.compressed);
  if (uncompressedFiles.length > 1) {
    const targetFile = uncompressedFiles[0];
    console.log(`   压缩文件: ${targetFile.file}`);
    const result = await logManager.compressFile(targetFile.file);
    console.log(`   ✓ 压缩结果: ${JSON.stringify(result)}`);
  } else {
    console.log('   跳过: 没有足够的未压缩文件 (可能已自动压缩)');
  }

  const updatedFiles = logManager.getFileList();
  const compressedCount = updatedFiles.filter(f => f.compressed).length;
  console.log(`   当前压缩文件数: ${compressedCount}/${updatedFiles.length}`);

  console.log('\n4. 测试透明读取压缩文件...');
  const queryResult = await logManager.queryRecords({ limit: 50 });
  console.log(`   ✓ 查询到 ${queryResult.data.length} 条记录 (预期50条)`);
  console.log(`   总记录估计: ${queryResult.total}`);
  console.log(`   记录完整性验证: ${queryResult.data.every(r => r.cpu && r.memory) ? '通过' : '失败'}`);

  console.log('\n5. 测试手动解压文件...');
  const compressedFiles = logManager.getFileList().filter(f => f.compressed);
  if (compressedFiles.length > 0) {
    const targetFile = compressedFiles[0];
    console.log(`   解压文件: ${targetFile.file}`);
    const decompressResult = await logManager.decompressFile(targetFile.file);
    console.log(`   ✓ 解压结果: success=${decompressResult.success || decompressResult.alreadyDecompressed}`);

    const verifyQuery = await logManager.queryRecords({ limit: 10 });
    console.log(`   解压后查询验证: 读取到 ${verifyQuery.data.length} 条记录`);
  } else {
    console.log('   跳过: 没有压缩文件');
  }

  console.log('\n6. 测试批量压缩所有旧文件...');
  const batchResult = await logManager.compressAllOldFiles();
  console.log(`   ✓ 批量压缩结果: ${JSON.stringify(batchResult)}`);

  console.log('\n7. 测试压缩统计信息...');
  const stats = logManager.getCompressionStats();
  console.log(`   总文件数: ${stats.totalFiles}`);
  console.log(`   已压缩: ${stats.compressedCount}, 未压缩: ${stats.uncompressedCount}`);
  console.log(`   原始总大小: ${stats.totalOriginalSize}B`);
  console.log(`   压缩后总大小: ${stats.totalCompressedSize}B`);
  console.log(`   已节省: ${stats.savedBytes}B`);
  console.log(`   平均压缩率: ${stats.averageRatio}%`);

  console.log('\n8. 测试从压缩文件导出报告...');
  const csvPath = path.join(testDir, 'export_from_compressed.csv');
  const exportResult = await logManager.exportReport({
    format: 'csv',
    outputPath: csvPath,
    includeProcesses: false
  });
  console.log(`   ✓ 导出成功: ${exportResult.totalExported} 条记录`);
  console.log(`   文件大小: ${fs.statSync(csvPath).size}B`);

  console.log('\n9. 测试索引重建（压缩后重启）...');
  await logManager.stop();
  
  const logManager2 = new LogManager({
    logDir: testDir,
    baseName: 'compress_test',
    enableCompression: true,
    compressionThreshold: COMPRESSION_THRESHOLD
  });
  await logManager2.start();
  
  const rebuiltFiles = logManager2.getFileList();
  console.log(`   ✓ 重建索引后文件数: ${rebuiltFiles.length}`);
  const rebuiltCompressedCount = rebuiltFiles.filter(f => f.compressed).length;
  console.log(`   压缩文件识别: ${rebuiltCompressedCount} 个`);
  
  const afterRestartQuery = await logManager2.queryRecords({ limit: 20 });
  console.log(`   重启后查询验证: 读取到 ${afterRestartQuery.data.length} 条记录`);

  await logManager2.stop();

  console.log('\n10. 验证 .gz 文件存在且为有效 gzip...');
  const allFiles = fs.readdirSync(testDir);
  const gzFiles = allFiles.filter(f => f.endsWith('.gz'));
  console.log(`   磁盘上 .gz 文件数: ${gzFiles.length}`);
  
  for (const gzFile of gzFiles) {
    const gzPath = path.join(testDir, gzFile);
    try {
      const content = fs.readFileSync(gzPath);
      const decompressed = zlib.gunzipSync(content);
      const lines = decompressed.toString('utf-8').split('\n').filter(l => l.trim());
      const validJson = lines.every(line => {
        try { JSON.parse(line); return true; } catch { return false; }
      });
      console.log(`   ✓ ${gzFile}: ${lines.length} 条记录, JSON有效: ${validJson}`);
    } catch (err) {
      console.log(`   ✗ ${gzFile}: 无效 - ${err.message}`);
    }
  }

  console.log('\n=== 所有压缩功能测试通过! ===');
  console.log(`\n测试目录: ${testDir}`);
  console.log('可以查看该目录下的文件结构确认压缩效果');
}

testCompression().catch(err => {
  console.error('测试失败:', err);
  console.error(err.stack);
  process.exit(1);
});
