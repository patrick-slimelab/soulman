using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using System.Linq;

namespace Soulman;

public class SyncServer : IHostedService, IDisposable
{
    private readonly ILogger<SyncServer> _logger;
    private readonly IOptionsMonitor<SoulmanSettings> _options;
    private TcpListener? _listener;
    private Task? _listeningTask;
    private readonly CancellationTokenSource _cts = new();

    public int Port { get; private set; }

    public SyncServer(ILogger<SyncServer> logger, IOptionsMonitor<SoulmanSettings> options)
    {
        _logger = logger;
        _options = options;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Try fixed port first, then dynamic
        try
        {
            _listener = new TcpListener(IPAddress.Any, 45833);
            _listener.Start();
        }
        catch
        {
            _listener = new TcpListener(IPAddress.Any, 0);
            _listener.Start();
        }

        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        
        _logger.LogInformation("SyncServer started on port {Port}", Port);

        _listeningTask = AcceptClientsAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        _listener?.Stop();
        if (_listeningTask != null)
        {
            try
            {
                await _listeningTask;
            }
            catch
            {
                // ignore
            }
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _cts.Dispose();
        _listener?.Stop();
    }

    private async Task AcceptClientsAsync(CancellationToken token)
    {
        if (_listener == null) return;

        while (!token.IsCancellationRequested)
        {
            try
            {
                var client = await _listener.AcceptTcpClientAsync(token);
                _ = Task.Run(() => HandleClientAsync(client, token), token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error accepting sync client");
            }
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken token)
    {
        using (client)
        await using (var stream = client.GetStream())
        using (var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true))
        await using (var writer = new StreamWriter(stream, Encoding.UTF8, leaveOpen: true) { AutoFlush = true })
        {
            _logger.LogDebug("Client connected: {Remote}", client.Client.RemoteEndPoint);

            try
            {
                while (!token.IsCancellationRequested && client.Connected)
                {
                    var line = await reader.ReadLineAsync(token);
                    if (line == null) break;

                    var parts = line.Split(' ', 2);
                    var command = parts[0].ToUpperInvariant();
                    var arg = parts.Length > 1 ? parts[1] : string.Empty;

                    switch (command)
                    {
                        case "LIST":
                            await HandleList(writer);
                            break;
                        case "GET":
                            await HandleGet(arg, writer, stream);
                            break;
                        case "BYE":
                            return;
                        default:
                            await writer.WriteLineAsync("ERROR Unknown command");
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error handling client {Remote}", client.Client.RemoteEndPoint);
            }
        }
    }

    private async Task HandleList(StreamWriter writer)
    {
        var settings = _options.CurrentValue;
        var roots = GetSyncRoots(settings);
        if (roots.Count == 0)
        {
            await writer.WriteLineAsync("[]");
            return;
        }

        var files = new List<object>();
        foreach (var root in roots)
        {
            if (!Directory.Exists(root.Path))
            {
                continue;
            }

            foreach (var f in Directory.EnumerateFiles(root.Path, "*", SearchOption.AllDirectories)
                         .Where(settings.IsSupportedFile))
            {
                var rel = Path.GetRelativePath(root.Path, f).Replace('\\', '/');
                var publishPath = string.IsNullOrWhiteSpace(root.Prefix)
                    ? rel
                    : $"{root.Prefix}/{rel}";

                files.Add(new
                {
                    Path = publishPath,
                    Size = new FileInfo(f).Length
                });
            }
        }

        var json = JsonSerializer.Serialize(files);
        await writer.WriteLineAsync(json);
    }

    private async Task HandleGet(string relativePath, StreamWriter writer, NetworkStream stream)
    {
        if (relativePath.Contains("../") || relativePath.Contains("..\\") || Path.IsPathRooted(relativePath))
        {
            await writer.WriteLineAsync("ERROR Invalid path");
            return;
        }

        var settings = _options.CurrentValue;
        var roots = GetSyncRoots(settings);
        if (roots.Count == 0)
        {
             await writer.WriteLineAsync("ERROR No library configured");
             return;
        }

        var (root, strippedRelativePath) = ResolveRootForRequestedPath(roots, relativePath);
        if (root == null)
        {
            await writer.WriteLineAsync("ERROR No matching sync root");
            return;
        }

        var fullPath = Path.Combine(root.Path, strippedRelativePath);
        if (!Path.GetFullPath(fullPath).StartsWith(Path.GetFullPath(root.Path), StringComparison.OrdinalIgnoreCase))
        {
             await writer.WriteLineAsync("ERROR Access denied");
             return;
        }

        if (!File.Exists(fullPath))
        {
            await writer.WriteLineAsync("ERROR File not found");
            return;
        }

        var info = new FileInfo(fullPath);
        var remoteLabel = stream.Socket?.RemoteEndPoint?.ToString() ?? "<unknown>";
        _logger.LogInformation("Serving {Path} ({Size} bytes) to {Remote}", relativePath, info.Length, remoteLabel);
        await writer.WriteLineAsync($"OK {info.Length}");
        await writer.FlushAsync();

        using var fileStream = File.OpenRead(fullPath);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await fileStream.CopyToAsync(stream);
            _logger.LogInformation("Finished serving {Path} in {Elapsed}ms", relativePath, sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error serving {Path} to {Remote}", relativePath, remoteLabel);
            throw;
        }
    }

    private static IReadOnlyList<SyncRoot> GetSyncRoots(SoulmanSettings settings)
    {
        // Legacy single-root mode still supported
        if (!string.IsNullOrWhiteSpace(settings.SyncRootPath))
        {
            return new List<SyncRoot>
            {
                new(settings.SyncRootPath!, string.Empty)
            };
        }

        var roots = new List<SyncRoot>();

        if (!string.IsNullOrWhiteSpace(settings.DestinationPath))
            roots.Add(new SyncRoot(settings.DestinationPath!, "Music"));

        if (!string.IsNullOrWhiteSpace(settings.MovieDestinationPath))
            roots.Add(new SyncRoot(settings.MovieDestinationPath!, "Movies"));

        if (!string.IsNullOrWhiteSpace(settings.TvDestinationPath))
            roots.Add(new SyncRoot(settings.TvDestinationPath!, "TV"));

        return roots
            .Select(r => new SyncRoot(Path.GetFullPath(r.Path), r.Prefix))
            .DistinctBy(r => r.Path)
            .ToList();
    }

    private static (SyncRoot? Root, string RelativePath) ResolveRootForRequestedPath(IReadOnlyList<SyncRoot> roots, string requestPath)
    {
        var normalized = requestPath.Replace('\\', '/').TrimStart('/');

        // Multi-root format: Prefix/path/to/file
        var slash = normalized.IndexOf('/');
        if (slash > 0)
        {
            var prefix = normalized[..slash];
            var rest = normalized[(slash + 1)..];
            var match = roots.FirstOrDefault(r => string.Equals(r.Prefix, prefix, StringComparison.OrdinalIgnoreCase));
            if (match != null)
            {
                return (match, rest);
            }
        }

        // Legacy/single-root fallback
        return (roots.FirstOrDefault(), normalized);
    }

    private sealed record SyncRoot(string Path, string Prefix);
}

