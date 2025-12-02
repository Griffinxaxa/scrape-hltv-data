# Cloudflare Protection Handling

## Solution: `cloudscraper` Library

This scraper uses the **`cloudscraper`** Python library (version 1.2.71) to automatically bypass Cloudflare protection on HLTV.org.

### How It Works

The `cloudscraper` library:
1. **Mimics browser behavior** - Automatically solves Cloudflare JavaScript challenges
2. **Maintains session state** - Handles cookies and headers properly
3. **TLS fingerprinting** - Uses correct TLS settings to appear as a real browser
4. **Automatic retry** - Handles rate limiting and temporary blocks

### Implementation

In the scraper code, it's used like this:

```python
import cloudscraper

# Create a cloudscraper session instead of regular requests
self.session = cloudscraper.create_scraper()

# Optionally set headers to mimic a real browser
self.session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
    # ... other headers
})

# Use it like a regular requests session
response = self.session.get(url, timeout=10)
```

### No Configuration Needed

The library automatically:
- Detects Cloudflare challenges
- Solves them in the background
- Maintains the session for subsequent requests
- Handles rate limiting gracefully

### Rate Limiting

Even with `cloudscraper`, HLTV may still rate limit requests. The scraper includes:
- Automatic retry with exponential backoff (10s, 20s, 30s delays)
- Respectful delays between requests (0.8s for player stats, 1s between matches)
- Detection of rate limit responses and automatic handling

### Alternative Solutions (Not Used)

We tested but did not use:
- **Playwright/Selenium** - Too slow for large-scale scraping
- **Manual cookie management** - Not reliable for automated scraping
- **Proxy rotation** - Not necessary with cloudscraper

### Dependencies

```bash
pip install cloudscraper==1.2.71
```

### References

- [cloudscraper GitHub](https://github.com/VeNoMouS/cloudscraper)
- [HLTV.org](https://www.hltv.org) - Data source







