<?php

declare(strict_types=1);

namespace Fairshare;

use Psr\Http\Message\ResponseFactoryInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;

/**
 * PSR-15 middleware (Slim, Mezzio, any PSR-15 stack): admit or refuse
 * incoming requests on behalf of the service that sent them.
 *
 *   $app->add(new Psr15Middleware($limiter, $responseFactory,
 *       service: fn ($req) => $req->getHeaderLine('x-service') ?: null, waitMs: 4000));
 */
final class Psr15Middleware implements MiddlewareInterface
{
    /** @var callable(ServerRequestInterface): ?string */
    private $service;

    public function __construct(
        private readonly FairShare $limiter,
        private readonly ResponseFactoryInterface $responses,
        ?callable $service = null,
        private readonly int $waitMs = 0,
        private readonly float $cost = 1.0,
        private readonly bool $failOpen = true,
    ) {
        $this->service = $service ?? static fn (ServerRequestInterface $r): ?string => $r->getHeaderLine('x-service') ?: null;
    }

    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        $service = ($this->service)($request);
        if ($service === null || $service === '') {
            return $handler->handle($request);
        }
        try {
            $d = $this->limiter->acquire($service, $this->cost, $this->waitMs);
        } catch (\Throwable $e) {
            if (!$this->failOpen) {
                return $this->json(503, ['error' => 'rate limiter unavailable']);
            }
            return $handler->handle($request)->withHeader('x-fair-share-mode', 'unavailable');
        }
        if (!$d->allowed) {
            return $this->json(429, ['error' => 'Too Many Requests', 'service' => $service, 'mode' => $d->mode, 'retryAfterMs' => $d->retryAfterMs])
                ->withHeader('Retry-After', (string) max(1, (int) ceil($d->retryAfterMs / 1000)))
                ->withHeader('x-fair-share-mode', $d->mode);
        }
        return $handler->handle($request)
            ->withHeader('x-fair-share-service', $service)
            ->withHeader('x-fair-share-mode', $d->mode)
            ->withHeader('x-fair-share-wait-ms', (string) $d->waitedMs);
    }

    private function json(int $status, array $body): ResponseInterface
    {
        $res = $this->responses->createResponse($status)->withHeader('content-type', 'application/json');
        $res->getBody()->write(json_encode($body, JSON_THROW_ON_ERROR));
        return $res;
    }
}
