# coregrep

## GitHub repositories with >250 stars

Fetch all repositories with more than 250 stars using the GitHub Search API, with pagination, rate-limit handling, and star-range partitioning to bypass the 1,000-result cap.

### Usage

1. Optionally export a token to increase rate limits:

```bash
export GITHUB_TOKEN=ghp_XXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

2. Run the fetcher:

```bash
python3 scripts/fetch_github_repos.py --min-stars 250 --out repos_over_250.jsonl --format jsonl
```

Flags:
- `--min-stars`: threshold; collects repos with strictly greater stars (default: 250)
- `--out`: output file path (default: `repos_over_250.jsonl`)
- `--format`: `jsonl` or `csv` (default: `jsonl`)
- `--token`: GitHub token (defaults to `$GITHUB_TOKEN`)
- `--max-repos`: optional cap for quick tests

Output example (JSON Lines):

```json
{"id": 28457823, "full_name": "freeCodeCamp/freeCodeCamp", "stargazers_count": 426774, "language": "TypeScript"}
```

### Referenced API

- GitHub Search API endpoint used: `https://api.github.com/search/repositories?q=stars:%3E250&sort=stars&order=desc&page=1&per_page=100`.

See: [`https://api.github.com/search/repositories?q=stars:%3E250&sort=stars&order=desc&page=1&per_page=100`](https://api.github.com/search/repositories?q=stars:%3E250&sort=stars&order=desc&page=1&per_page=100)

