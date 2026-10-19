<?php

declare(strict_types=1);

namespace Fairshare;

use Closure;

/**
 * Laravel middleware. Register it in bootstrap/app.php or the HTTP kernel:
 *
 *   $middleware->alias(['fairshare' => \Fairshare\LaravelMiddleware::class]);
 *   Route::middleware('fairshare:thirdparty,4000')->group(...);
 *
 * Bind a FairShare per upstream in a service provider:
 *   $this->app->bind('fairshare.thirdparty', fn () => new FairShare(new \Predis\Client(config('database.redis.default')), 'thirdparty'));
 */
final class LaravelMiddleware
{
    public function handle($request, Closure $next, string $upstream, int $waitMs = 0, string $header = 'x-service')
    {
        $service = $request->header($header);
        if (!$service) {
            return $next($request);
        }
        /** @var FairShare $limiter */
        $limiter = app("fairshare.$upstream");
        try {
            $d = $limiter->acquire($service, 1.0, $waitMs);
        } catch (\Throwable $e) {
            return $next($request)->header('x-fair-share-mode', 'unavailable');
        }
        if (!$d->allowed) {
            return response()->json(['error' => 'Too Many Requests', 'service' => $service, 'mode' => $d->mode, 'retryAfterMs' => $d->retryAfterMs], 429)
                ->header('Retry-After', (string) max(1, (int) ceil($d->retryAfterMs / 1000)))
                ->header('x-fair-share-mode', $d->mode);
        }
        return $next($request)
            ->header('x-fair-share-service', $service)
            ->header('x-fair-share-mode', $d->mode)
            ->header('x-fair-share-wait-ms', (string) $d->waitedMs);
    }
}
