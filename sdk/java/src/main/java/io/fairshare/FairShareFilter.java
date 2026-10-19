package io.fairshare;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Function;

/**
 * Servlet filter: admit or refuse incoming requests on behalf of the service
 * that sent them. Works in Spring Boot via a {@code FilterRegistrationBean},
 * and in any Jakarta servlet container.
 *
 * <pre>{@code
 * @Bean FilterRegistrationBean<FairShareFilter> fairShare(FairShare limiter) {
 *     var reg = new FilterRegistrationBean<>(new FairShareFilter(limiter, r -> r.getHeader("X-Service"), Duration.ofSeconds(4)));
 *     reg.addUrlPatterns("/api/*");
 *     return reg;
 * }
 * }</pre>
 */
public final class FairShareFilter implements Filter {
    private final FairShare limiter;
    private final Function<HttpServletRequest, String> service;
    private final Duration wait;
    private final double cost;
    private final boolean failOpen;

    public FairShareFilter(FairShare limiter, Function<HttpServletRequest, String> service, Duration wait) {
        this(limiter, service, wait, 1, true);
    }

    public FairShareFilter(FairShare limiter, Function<HttpServletRequest, String> service, Duration wait, double cost, boolean failOpen) {
        this.limiter = limiter;
        this.service = service;
        this.wait = wait;
        this.cost = cost;
        this.failOpen = failOpen;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (!(request instanceof HttpServletRequest req) || !(response instanceof HttpServletResponse res)) {
            chain.doFilter(request, response);
            return;
        }
        String svc = service.apply(req);
        if (svc == null || svc.isEmpty()) {
            chain.doFilter(request, response);
            return;
        }
        Decision d;
        try {
            d = limiter.acquire(svc, cost, wait);
        } catch (RuntimeException e) {
            if (!failOpen) {
                res.sendError(503, "rate limiter unavailable");
                return;
            }
            res.setHeader("X-Fair-Share-Mode", "unavailable");
            chain.doFilter(request, response);
            return;
        }
        res.setHeader("X-Fair-Share-Service", svc);
        res.setHeader("X-Fair-Share-Mode", d.mode());
        res.setHeader("X-Fair-Share-Wait-Ms", Long.toString(d.waited().toMillis()));
        if (!d.allowed()) {
            res.setHeader("Retry-After", Long.toString(Math.max(1, (d.retryAfter().toMillis() + 999) / 1000)));
            res.setStatus(429);
            res.setContentType("application/json");
            res.getWriter().write("{\"error\":\"Too Many Requests\",\"service\":\"" + svc + "\",\"mode\":\"" + d.mode() + "\",\"retryAfterMs\":" + d.retryAfter().toMillis() + "}");
            return;
        }
        chain.doFilter(request, response);
    }
}
