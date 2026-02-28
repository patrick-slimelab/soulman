using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;

namespace Soulman;

public class SyncClient
{
    private readonly ILogger<SyncClient> _logger;
    private readonly IOptionsMonitor<SoulmanSettings> _options;
    private readonly MoveLogStore _moveLog;
    private readonly MoveNotificationBroker _moveBroker;
    private readonly TransferProgressBroker _progressBroker;

    public SyncClient(
        ILogger<SyncClient> logger,
        IOptionsMonitor<SoulmanSettings> options,
        MoveLogStore moveLog,
        MoveNotificationBroker moveBroker,
        TransferProgressBroker progressBroker)
    {
        _logger = logger;
        _options = options;
        _moveLog = moveLog;
        _moveBroker = moveBroker;
        _progressBroker = progressBroker;
    }

    public async Task SyncWithPeerAsync(DiscoveredInstance peer, CancellationToken token)
    {
        if (peer.SyncPort <= 0) return;

        var syncWatch = Stopwatch.StartNew();
        _logger.LogInformation("Starting sync with {Machine} at {Endpoint}:{Port}", peer.MachineName, peer.EndPoint.Address, peer.SyncPort);

        List<RemoteFile>? remoteFiles = null;

        // 1. Fetch File List
        try
        {
            using var client = new TcpClient();
            // Use a specific timeout for the initial connection and list retrieval
            using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            connectCts.CancelAfter(TimeSpan.FromSeconds(15));

            await client.ConnectAsync(peer.EndPoint.Address, peer.SyncPort, connectCts.Token);

            await using var stream = client.GetStream();
            using var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);
            await using var writer = new StreamWriter(stream, Encoding.UTF8, leaveOpen: true) { AutoFlush = true };

            await writer.WriteLineAsync("LIST");
            var json = await reader.ReadLineAsync(connectCts.Token);

            if (!string.IsNullOrEmpty(json))
            {
                remoteFiles = JsonSerializer.Deserialize<List<RemoteFile>>(json);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to fetch file list from {Machine}", peer.MachineName);
            return;
        }

        if (remoteFiles == null || remoteFiles.Count == 0)
        {
            _logger.LogInformation("Peer {Machine} returned no files", peer.MachineName);
            return;
        }

        _logger.LogInformation("Peer {Machine} has {Count} files", peer.MachineName, remoteFiles.Count);

        var settings = _options.CurrentValue;
        var destination = !string.IsNullOrWhiteSpace(settings.SyncRootPath)
            ? settings.SyncRootPath
            : settings.DestinationPath;
        if (string.IsNullOrEmpty(destination) && string.IsNullOrEmpty(settings.MovieDestinationPath) && string.IsNullOrEmpty(settings.TvDestinationPath)) return;

        int syncedCount = 0;
        TcpClient? transferClient = null;
        StreamReader? transferReader = null;
        StreamWriter? transferWriter = null;

        try
        {
            foreach (var file in remoteFiles)
            {
                if (token.IsCancellationRequested) break;

                var localPath = ResolveLocalPath(settings, destination, file.Path);
                if (File.Exists(localPath))
                {
                    _logger.LogDebug("Skipping existing file {Path}", file.Path);
                    continue;
                }

                int retryCount = 0;
                const int MaxRetries = 5;

                while (retryCount < MaxRetries && !token.IsCancellationRequested)
                {
                    try
                    {
                        // Ensure connection
                        if (transferClient == null || !transferClient.Connected)
                        {
                            transferClient = new TcpClient();
                            transferClient.ReceiveTimeout = 60000;
                            transferClient.SendTimeout = 60000;
                            await transferClient.ConnectAsync(peer.EndPoint.Address, peer.SyncPort, token);

                            var stream = transferClient.GetStream();
                            transferReader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);
                            transferWriter = new StreamWriter(stream, Encoding.UTF8, leaveOpen: true) { AutoFlush = true };
                        }

                        _logger.LogInformation("Downloading {Path} ({Size} bytes) [Attempt {Attempt}/{Max}]", file.Path, file.Size, retryCount + 1, MaxRetries);

                        await transferWriter!.WriteLineAsync($"GET {file.Path}");
                        var response = await transferReader!.ReadLineAsync(token);

                        if (response != null && response.StartsWith("OK"))
                        {
                            var sizePart = response.Split(' ').Skip(1).FirstOrDefault();
                            if (long.TryParse(sizePart, out var size))
                            {
                                var fileWatch = Stopwatch.StartNew();
                                await DownloadFileAsync(transferClient.GetStream(), localPath, size, token);
                                
                                _moveLog.Add(new MoveEntry(DateTimeOffset.UtcNow, $"Peer://{peer.MachineName}/{file.Path}", localPath, Array.Empty<string>()));
                                syncedCount++;
                                _logger.LogInformation("Finished {Path} ({Size} bytes) in {Elapsed}ms", file.Path, size, fileWatch.ElapsedMilliseconds);
                                break; // Success, exit retry loop
                            }
                        }
                        else
                        {
                            _logger.LogWarning("Failed to initiate download for {Path}: {Response}", file.Path, response);
                            break; // Server rejected, do not retry
                        }
                    }
                    catch (Exception ex)
                    {
                        retryCount++;
                        _logger.LogWarning("Download failed for {Path}: {Message}. Retrying in 2s... ({Attempt}/{Max})", file.Path, ex.Message, retryCount, MaxRetries);

                        // Disconnect and force reconnect next attempt
                        try { transferClient?.Dispose(); } catch { }
                        transferClient = null;
                        transferReader = null;
                        transferWriter = null;

                        if (retryCount >= MaxRetries)
                        {
                            _logger.LogError("Giving up on {Path} after {Max} attempts", file.Path, MaxRetries);
                        }
                        else
                        {
                            await Task.Delay(2000, token);
                        }
                    }
                }
            }

            if (transferWriter != null)
            {
                try { await transferWriter.WriteLineAsync("BYE"); } catch { }
            }
            
            _logger.LogInformation("Sync with {Machine} complete in {Elapsed}s. Downloaded {Count} files.", peer.MachineName, syncWatch.Elapsed.TotalSeconds, syncedCount);

            if (syncedCount > 0)
            {
                var notifyTarget = settings.SyncRootPath
                    ?? settings.DestinationPath
                    ?? settings.MovieDestinationPath
                    ?? settings.TvDestinationPath
                    ?? "<unset>";
                _moveBroker.Publish(syncedCount, notifyTarget);
            }
        }
        finally
        {
            transferClient?.Dispose();
        }
    }

    private async Task DownloadFileAsync(NetworkStream stream, string localPath, long size, CancellationToken token)
    {
        var dir = Path.GetDirectoryName(localPath);
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }

        var fileName = Path.GetFileName(localPath);
        _progressBroker.Report(fileName, 0, size);

        // Download to temp file first
        var tempPath = localPath + ".tmp";
        try
        {
            using (var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                var buffer = new byte[81920]; // 80KB buffer
                long remaining = size;
                long totalRead = 0;

                // Reusable CTS for performance
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);

                while (remaining > 0)
                {
                    var readSize = (int)Math.Min(remaining, buffer.Length);

                    // Reset timeout timer
                    timeoutCts.CancelAfter(TimeSpan.FromSeconds(60));

                    int read;
                    try
                    {
                        read = await stream.ReadAsync(buffer, 0, readSize, timeoutCts.Token);
                    }
                    catch (OperationCanceledException) when (!token.IsCancellationRequested && timeoutCts.IsCancellationRequested)
                    {
                        // Regenerate CTS if it was cancelled by timeout so we can reuse/cleanly exit (though we throw here)
                        throw new IOException("Read timed out (60s without data)");
                    }

                    if (read == 0) throw new IOException("Unexpected end of stream");
                    await fileStream.WriteAsync(buffer, 0, read, token);
                    remaining -= read;
                    totalRead += read;

                    _progressBroker.Report(fileName, totalRead, size);
                }

                if (totalRead != size)
                {
                    throw new IOException($"Incomplete download for {fileName}: expected {size} bytes, got {totalRead}");
                }
            }

            File.Move(tempPath, localPath, overwrite: true);
            _progressBroker.ReportCompletion(fileName);
        }
        catch
        {
            if (File.Exists(tempPath)) File.Delete(tempPath);
            throw;
        }
    }

    private static string ResolveLocalPath(SoulmanSettings settings, string? legacyDestination, string remotePath)
    {
        var normalized = remotePath.Replace('\\', '/').TrimStart('/');

        // If SyncRootPath is configured, preserve legacy single-root behavior
        if (!string.IsNullOrWhiteSpace(settings.SyncRootPath))
        {
            return Path.Combine(settings.SyncRootPath!, normalized);
        }

        var slash = normalized.IndexOf('/');
        if (slash > 0)
        {
            var prefix = normalized[..slash];
            var rest = normalized[(slash + 1)..];

            if (string.Equals(prefix, "Music", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(settings.DestinationPath))
                return Path.Combine(settings.DestinationPath!, rest);

            if (string.Equals(prefix, "Movies", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(settings.MovieDestinationPath))
                return Path.Combine(settings.MovieDestinationPath!, rest);

            if (string.Equals(prefix, "TV", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(settings.TvDestinationPath))
                return Path.Combine(settings.TvDestinationPath!, rest);
        }

        if (!string.IsNullOrWhiteSpace(legacyDestination))
        {
            return Path.Combine(legacyDestination, normalized);
        }

        if (!string.IsNullOrWhiteSpace(settings.DestinationPath))
        {
            return Path.Combine(settings.DestinationPath!, normalized);
        }

        // Final fallback should be very rare
        return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), normalized);
    }

    private class RemoteFile
    {
        public string Path { get; set; } = "";
        public long Size { get; set; }
    }
}
