using Microsoft.Playwright;
using System.Text.Json;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using MySqlConnector;
using System.Globalization;
using System.Linq;
using System.Text.Encodings.Web; // keep "+" unescaped in JSON
using System.Net;
using System.Net.Http;
using System.Text.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MarathonbetScrapper
{
    public static class MarathonbetScraper
    {
        private static readonly HttpClient _http = new(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
        })
        {
            Timeout = TimeSpan.FromSeconds(25)
        };

        private const string POST_ENDPOINT = "https://www.hh24tech.com/connector/index.php";

        private static async Task<(bool ok, string? body)> PostJsonWithRetryAsync<T>(
            string url, T payload, int maxAttempts = 4)
        {
            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions
            {
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
                WriteIndented = false
            });

            Exception? lastEx = null;

            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    using var content = new StringContent(json, Encoding.UTF8, "application/json");
                    using var resp = await _http.PostAsync(url, content);
                    var respBody = await resp.Content.ReadAsStringAsync();

                    if ((int)resp.StatusCode >= 200 && (int)resp.StatusCode < 300)
                        return (true, respBody);

                    if (resp.StatusCode is HttpStatusCode.RequestTimeout
                        or HttpStatusCode.TooManyRequests
                        or HttpStatusCode.BadGateway
                        or HttpStatusCode.ServiceUnavailable
                        or HttpStatusCode.GatewayTimeout)
                    {
                        await Task.Delay(400 * attempt * attempt + Random.Shared.Next(0, 250));
                        continue;
                    }

                    return (false, $"HTTP {(int)resp.StatusCode}: {resp.ReasonPhrase}; body: {respBody}");
                }
                catch (Exception ex)
                {
                    lastEx = ex;
                    await Task.Delay(400 * attempt * attempt + Random.Shared.Next(0, 250));
                }
            }

            return (false, $"post-failed after {maxAttempts} attempts: {lastEx?.Message}");
        }

        public static async Task RunAsync(string? startSport = null, bool allowDirectFallback = false)
        {
            Console.WriteLine("Launching browser...");

            var playwright = await Playwright.CreateAsync();
            var browser = await playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
            {
                Headless = false,
                SlowMo = 1000,
                Devtools = true
            });

            var context = await browser.NewContextAsync();
            var page = await context.NewPageAsync();

            Console.WriteLine("Navigating to Marathonbet...");
            await page.GotoAsync("https://www.marathonbet.it/scommesse", new() { WaitUntil = WaitUntilState.NetworkIdle });
            await HandleRandomPopup(page);

            // Ensure the sidebar is fully rendered / scrolled so we don't miss sports
            await EnsureAllSportsVisibleAsync(page);

            // 1) Read allowed sports from page (canonicalized)
            var sports = await GetAllowedSports(page); // -> subset of ["Calcio","Pallacanestro","Tennis","Rugby"]

            // 2) If caller requested a specific sport, narrow to that
            if (!string.IsNullOrWhiteSpace(startSport))
            {
                var wanted = CanonicalFromTextOrHref(startSport, null);
                if (wanted != null)
                    sports = sports.Where(s => s.Equals(wanted, StringComparison.OrdinalIgnoreCase)).ToList();
                else
                    sports = new List<string>();
            }

            if (sports.Count == 0)
            {
                Console.WriteLine("No matching sports after filtering (Calcio/Pallacanestro/Tennis/Rugby).");
                if (!string.IsNullOrWhiteSpace(startSport))
                {
                    Console.WriteLine($"Requested sport '{startSport}' is not visible in the sidebar → stopping to avoid direct-navigation (bot risk).");

                    // Optional fallback
                    if (allowDirectFallback)
                    {
                        var s = Canon(startSport);
                        string? url = s switch
                        {
                            var x when CalcioSyn.Contains(x) => "https://www.marathonbet.it/scommesse/calcio",
                            var x when BasketSyn.Contains(x) => "https://www.marathonbet.it/scommesse/pallacanestro",
                            var x when TennisSyn.Contains(x) => "https://www.marathonbet.it/scommesse/tennis",
                            var x when RugbySyn.Contains(x)  => "https://www.marathonbet.it/scommesse/rugby",
                            _ => null
                        };
                        if (url != null)
                        {
                            Console.WriteLine($"[Fallback enabled] Navigating directly to: {url}");
                            await page.GotoAsync(url, new() { WaitUntil = WaitUntilState.NetworkIdle });
                            await HandleRandomPopup(page);
                            sports = new List<string>
                            {
                                url.Contains("/calcio") ? "Calcio" :
                                url.Contains("/pallacanestro") ? "Pallacanestro" :
                                url.Contains("/tennis") ? "Tennis" :
                                url.Contains("/rugby") ? "Rugby" : "Calcio"
                            };
                        }
                    }
                }

                if (sports.Count == 0) return; // stop cleanly
            }

            Console.WriteLine($"Sports to process (after filtering): {string.Join(", ", sports)}");

            foreach (var sport in sports)
            {
                Console.WriteLine($"\n=== Processing sport: {sport} ===");
                await ClickSportSection(page, sport);     // only clicks if visible
                await HandleRandomPopup(page);

                var leagues = await GetLeaguesForSport(page);

                foreach (var league in leagues)
                {
                    Console.WriteLine($"\n--- Processing league: {league} ---");

                    await HandleRandomPopup(page);
                    // Rugby-only: pause after fixtures load
                    await ClickLeagueAndLoadOdds(page, league,
                        sport.Equals("Rugby", StringComparison.OrdinalIgnoreCase));
                    await HandleRandomPopup(page);

                    var safeSport = MakeSafeFilePart(sport);
                    var safeLeague = MakeSafeFilePart(league).Replace(" ", "_");
                    try
                    {
                        var matches = await ExtractOddsAsync(page, $"{safeSport}_{safeLeague}.json");
                        await DbSaver.SaveOddsAsync("marathonbet", sport, league, matches);
                        Console.WriteLine($"Inserted {matches.Count} matches into DB.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("DB save failed: " + ex.Message);
                    }

                    // Go back and re-enter the SAME sport (again via visible click only)
                    await page.GotoAsync("https://www.marathonbet.it/scommesse", new() { WaitUntil = WaitUntilState.NetworkIdle });
                    await HandleRandomPopup(page);
                    await EnsureAllSportsVisibleAsync(page);
                    await ClickSportSection(page, sport);
                    await HandleRandomPopup(page);
                }
            }
        }

        const string ConnStr =
            "Server=mysql.aruba.it;Port=3306;Database=Sql1764243_4;User ID=Sql1764243;Password=Isolatof10!;SslMode=None;";

        public static class DbSaver
        {
            static decimal ToDec(string s) =>
                decimal.Parse(s.Replace(',', '.'), CultureInfo.InvariantCulture);

            static (string home, string away) SplitTeams(string teams)
            {
                var p = teams.Split(" vs ", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                return (p.Length >= 1 ? p[0] : "UNKNOWN", p.Length >= 2 ? p[1] : "UNKNOWN");
            }

            public static async Task SaveOddsAsync(
                string bookmaker, string sport, string league, List<MarathonbetScraper.MatchData> matches)
            {
                const string sql = @"
INSERT INTO odds
(bookmaker,sport,league,home_team,away_team,market_type,market_line,selection,odds)
VALUES
(@book,@sport,@league,@home,@away,@mtype,@mline,@sel,@odds);";

                await using var conn = new MySqlConnection(ConnStr);
                await conn.OpenAsync();

                foreach (var m in matches)
                {
                    var (home, away) = SplitTeams(m.Teams);

                    // base odds (market_type = label, selection = label, market_line = NULL)
                    foreach (var kv in m.Odds)
                    {
                        await InsertAsync(conn, sql, bookmaker, sport, league, home, away,
                            kv.Key, null, kv.Key, kv.Value);
                    }

                    // O/U (kept for non-Rugby sports if they populate OUTotals)
                    foreach (var (line, sides) in m.OUTotals)
                    {
                        foreach (var pick in new[] { "UNDER", "OVER" })
                        {
                            if (sides.TryGetValue(pick, out var val))
                                await InsertAsync(conn, sql, bookmaker, sport, league, home, away,
                                    "OU", line, pick, val);
                        }
                    }

                    // TT Handicap (kept for non-Rugby if present)
                    foreach (var (line, sides) in m.TTPlusHandicap)
                    {
                        foreach (var pick in new[] { "1", "2" })
                        {
                            if (sides.TryGetValue(pick, out var val))
                                await InsertAsync(conn, sql, bookmaker, sport, league, home, away,
                                    "TT_HANDICAP", line, pick, val);
                        }
                    }
                }
            }

            static async Task InsertAsync(MySqlConnection conn, string sql,
                string bookmaker, string sport, string league, string home, string away,
                string marketType, string? line, string sel, string val)
            {
                await using var cmd = new MySqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@book", bookmaker);
                cmd.Parameters.AddWithValue("@sport", sport);
                cmd.Parameters.AddWithValue("@league", league);
                cmd.Parameters.AddWithValue("@home", home);
                cmd.Parameters.AddWithValue("@away", away);
                cmd.Parameters.AddWithValue("@mtype", marketType);
                cmd.Parameters.AddWithValue("@mline", string.IsNullOrEmpty(line) ? (object)DBNull.Value : ToDec(line));
                cmd.Parameters.AddWithValue("@sel", sel);
                cmd.Parameters.AddWithValue("@odds", ToDec(val));
                await cmd.ExecuteNonQueryAsync();
            }
        }

        // =========================
        // TT+ HANDICAP — HELPERS (kept for other sports)
        // =========================

        private static string BuildHcpSummary(Dictionary<string, Dictionary<string, string>> hcp)
        {
            if (hcp == null || hcp.Count == 0) return "";
            var parsed = new SortedDictionary<double, (string One, string Two)>();
            var fallback = new SortedDictionary<string, (string One, string Two)>(StringComparer.Ordinal);

            foreach (var (lineKey, sides) in hcp)
            {
                var one = sides.TryGetValue("1", out var v1) ? v1 : null;
                var two = sides.TryGetValue("2", out var v2) ? v2 : null;

                if (double.TryParse(lineKey.Replace(',', '.'),
                                    NumberStyles.Float,
                                    CultureInfo.InvariantCulture,
                                    out var lineNum))
                {
                    parsed[lineNum] = (one, two);
                }
                else
                {
                    fallback[lineKey] = (one, two);
                }
            }

            var parts = new List<string>();
            foreach (var kv in parsed)
                parts.Add($"{kv.Key.ToString("0.##", CultureInfo.InvariantCulture)} (1: {kv.Value.One ?? "-"}, 2: {kv.Value.Two ?? "-"})");
            foreach (var kv in fallback)
                parts.Add($"{kv.Key} (1: {kv.Value.One ?? "-"}, 2: {kv.Value.Two ?? "-"})");

            return string.Join(" | ", parts);
        }

        private static string NormalizeHcpLine(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return "";
            var t = s.Trim().Replace(",", ".").Replace('\u00A0', ' ');
            var m = Regex.Match(t, @"[+\-]?\d+(?:\.\d+)?");
            return m.Success ? m.Value : t;
        }

        // Parse nearest TT menu (kept for other sports)
        private static async Task<List<(string line, string one, string two)>> ParseNearestTTMenuAsync(
            IPage page, ILocator ttButton)
        {
            try { await page.WaitForSelectorAsync(".dropdown-menu.show", new() { State = WaitForSelectorState.Visible, Timeout = 800 }); } catch { }

            var btnHandle = await ttButton.ElementHandleAsync();
            if (btnHandle == null) return new();

            string json = await page.EvaluateAsync<string>(@"(button) => {
      const T = s => (s||'').toString().trim();
      const vis = el => {
        if (!el) return false;
        const s = getComputedStyle(el), r = el.getBoundingClientRect();
        return s.visibility !== 'hidden' && s.display !== 'none' && r.width > 0 && r.height > 0;
      };
      const center = el => { const r = el.getBoundingClientRect(); return {x:(r.left+r.right)/2, y:(r.top+r.bottom)/2}; };
      const btnC = center(button);

      const menus = Array.from(document.querySelectorAll('.dropdown-menu.show')).filter(vis);
      if (!menus.length) return '[]';
      let box = null, best = Infinity;
      for (const m of menus) {
        const c = center(m), d2 = (c.x - btnC.x)**2 + (c.y - btnC.y)**2;
        if (d2 < best) { best = d2; box = m; }
      }
      if (!box) return '[]';

      let lists = Array.from(box.querySelectorAll('.dropdown-content .lista-quote.colonne-3')).filter(vis);
      lists = lists.filter(l => l.querySelector('a.dropdown-item'));
      if (!lists.length) lists = Array.from(box.querySelectorAll('.lista-quote.colonne-3')).filter(vis).filter(l => l.querySelector('a.dropdown-item'));
      if (!lists.length) return '[]';

      let list = null, b = Infinity;
      for (const L of lists) {
        const c = center(L), d2 = (c.x - btnC.x)**2 + (c.y - btnC.y)**2;
        if (d2 < b) { b = d2; list = L; }
      }
      if (!list) return '[]';

      const kids = Array.from(list.children || []);
      const out = [];
      for (let i = 0; i < kids.length; i++) {
        const node = kids[i];
        if (!(node.matches && node.matches('a.dropdown-item'))) continue;

        const rawLine = T(node.textContent);

        let one = '', two = '', seen = 0;
        for (let j = i + 1; j < kids.length && seen < 2; j++) {
          const n = kids[j];
          if (!(n.matches && n.matches('.contenitoreSingolaQuota'))) continue;

          const lab = T(n.querySelector('.titoloQuotazione')?.textContent).toUpperCase();
          const val = T(n.querySelector('.tipoQuotazione_1')?.textContent);

          if (lab == '1' && !one) { one = val; seen++; }
          else if (lab == '2' && !two) { two = val; seen++; }
        }
        if (rawLine && (one || two)) out.push([rawLine, one, two]);
      }
      return JSON.stringify(out);
    }", btnHandle);

            var lines = new List<(string, string, string)>();
            try
            {
                using var doc = JsonDocument.Parse(json);
                foreach (var item in doc.RootElement.EnumerateArray())
                {
                    var raw = item.GetArrayLength() > 0 ? item[0].GetString() ?? "" : "";
                    var one = item.GetArrayLength() > 1 ? item[1].GetString() ?? "" : "";
                    var two = item.GetArrayLength() > 2 ? item[2].GetString() ?? "" : "";
                    if (!string.IsNullOrWhiteSpace(raw) && (!string.IsNullOrWhiteSpace(one) || !string.IsNullOrWhiteSpace(two)))
                        lines.Add((raw, one, two));
                }
            }
            catch { }
            return lines;
        }

        // =========================
        // END TT+ HELPERS
        // =========================

        private static async Task EnsureAllSportsVisibleAsync(IPage page)
        {
            try
            {
                var container = page.Locator(".elemento-competizioni-widget").First;
                if (await container.IsVisibleAsync(new() { Timeout = 1500 }))
                {
                    for (int i = 0; i < 6; i++)
                    {
                        try { await container.EvaluateAsync("el => el.scrollBy(0, el.clientHeight)"); } catch { }
                        await page.WaitForTimeoutAsync(200);
                    }
                }
            }
            catch { }
        }

        private static string StripDiacritics(string s)
        {
            var norm = s.Normalize(NormalizationForm.FormD);
            var sb = new StringBuilder();
            foreach (var c in norm)
            {
                var uc = CharUnicodeInfo.GetUnicodeCategory(c);
                if (uc != UnicodeCategory.NonSpacingMark) sb.Append(c);
            }
            return sb.ToString().Normalize(NormalizationForm.FormC);
        }

        private static string Canon(string s)
        {
            s = (s ?? "")
                .Replace('\u00A0', ' ')
                .Trim();

            s = Regex.Replace(s, @"\s*\(\d+\)\s*$", "");
            s = StripDiacritics(s).ToUpperInvariant();
            s = Regex.Replace(s, @"\s+", " ");
            return s;
        }

        // Allowed sets
        private static readonly HashSet<string> CalcioSyn = new(StringComparer.OrdinalIgnoreCase)
        { "CALCIO", "SOCCER", "FOOTBALL" };

        private static readonly HashSet<string> BasketSyn = new(StringComparer.OrdinalIgnoreCase)
        { "BASKET", "BASKETBALL", "PALLACANESTRO" };

        private static readonly HashSet<string> TennisSyn = new(StringComparer.OrdinalIgnoreCase)
        { "TENNIS" };

        // Rugby
        private static readonly HashSet<string> RugbySyn = new(StringComparer.OrdinalIgnoreCase)
        { "RUGBY", "RUGBY UNION", "RUGBY LEAGUE" };

        private static readonly string[] CalcioHrefKeys = { "/calcio", "/football", "/soccer" };
        private static readonly string[] BasketHrefKeys = { "/pallacanestro", "/basketball" };
        private static readonly string[] TennisHrefKeys = { "/tennis" };
        private static readonly string[] RugbyHrefKeys = { "/rugby" };

        private static string? CanonicalFromTextOrHref(string text, string? href)
        {
            var c = Canon(text);

            // text match
            if (CalcioSyn.Contains(c)) return "Calcio";
            if (BasketSyn.Contains(c)) return "Pallacanestro";
            if (TennisSyn.Contains(c)) return "Tennis";
            if (RugbySyn.Contains(c)) return "Rugby";

            // href match
            href = (href ?? "").ToLowerInvariant();
            if (CalcioHrefKeys.Any(k => href.Contains(k))) return "Calcio";
            if (BasketHrefKeys.Any(k => href.Contains(k))) return "Pallacanestro";
            if (TennisHrefKeys.Any(k => href.Contains(k))) return "Tennis";
            if (RugbyHrefKeys.Any(k => href.Contains(k))) return "Rugby";

            return null;
        }

        private static string MapAliasToSite(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return "";
            var L = s.Trim().ToUpperInvariant();
            if (L is "SOCCER" or "FOOTBALL" or "CALCIO") return "Calcio";
            if (L is "BASKET" or "BASKETBALL" or "PALLACANESTRO") return "Pallacanestro";
            if (L is "TENNIS") return "Tennis";
            if (L.StartsWith("RUGBY")) return "Rugby";
            return s.Trim(); // fallback
        }

        private static bool IsAllowedSiteLabel(string s)
        {
            var m = MapAliasToSite(s);
            return m is "Calcio" or "Pallacanestro" or "Tennis" or "Rugby";
        }

        private static string Norm(string s) =>
            (s ?? "").Trim().ToUpperInvariant()
                .Replace("À", "A").Replace("È", "E").Replace("É", "E")
                .Replace("Ì", "I").Replace("Ò", "O").Replace("Ù", "U");

        private static bool IsAllowedSport(string label)
        {
            var L = Norm(label);
            return L is "CALCIO" or "SOCCER" or "FOOTBALL"
                   or "TENNIS"
                   or "PALLACANESTRO"
                   or "RUGBY" or "RUGBY UNION" or "RUGBY LEAGUE";
        }

        private static List<string> ReorderToStart(List<string> labels, string? startSport)
        {
            if (labels.Count == 0) return labels;

            var preferred = new[] { "Calcio", "Pallacanestro", "Tennis", "Rugby" };

            string MapAlias(string s)
            {
                var L = s.Trim();
                if (L.Equals("Soccer", StringComparison.OrdinalIgnoreCase) ||
                    L.Equals("Football", StringComparison.OrdinalIgnoreCase)) return "Calcio";
                if (L.Equals("Basketball", StringComparison.OrdinalIgnoreCase)) return "Pallacanestro";
                if (L.StartsWith("Rugby", StringComparison.OrdinalIgnoreCase)) return "Rugby";
                return L;
            }

            labels = labels
                .OrderBy(l =>
                {
                    var idx = Array.IndexOf(preferred, MapAlias(l));
                    return idx < 0 ? int.MaxValue : idx;
                })
                .ToList();

            if (!string.IsNullOrWhiteSpace(startSport))
            {
                var wanted = MapAlias(startSport);
                int idx = labels.FindIndex(l => l.Equals(wanted, StringComparison.OrdinalIgnoreCase));
                if (idx > 0) labels = labels.Skip(idx).Concat(labels.Take(idx)).ToList();
            }

            return labels;
        }

        private static string MakeSafeFilePart(string s)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var cleaned = string.Join("_", s.Split(invalid, StringSplitOptions.RemoveEmptyEntries)).Trim('_');
            return Regex.Replace(cleaned, "_{2,}", "_");
        }

        private static async Task<List<(string Text, string? Href)>> GetSportsRaw(IPage page)
        {
            var a = page.Locator(".card.elemento-competizioni-widget .titolo-accordion a");
            int n = await a.CountAsync();
            var list = new List<(string, string?)>(n);

            for (int i = 0; i < n; i++)
            {
                var link = a.Nth(i);
                string text = (await link.InnerTextAsync()).Trim();
                string? href = null;
                               try { href = await link.GetAttributeAsync("href"); } catch { }
                list.Add((text, href));
            }
            return list;
        }

        private static async Task<List<string>> GetAllowedSports(IPage page)
        {
            var raw = await GetSportsRaw(page);

            Console.WriteLine("Sports detected (text | href):");
            foreach (var (t, h) in raw)
                Console.WriteLine($"  - \"{t}\" | {h}");

            var mapped = raw
                .Select(x => CanonicalFromTextOrHref(x.Text, x.Href))
                .Where(x => x != null)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .Select(x => x!)
                .ToList();

            mapped = ReorderToStart(mapped, null);

            return mapped; // may contain: "Calcio", "Pallacanestro", "Tennis", "Rugby"
        }

        private static bool UrlLooksLikeSport(string url, string canonicalSport)
        {
            var u = (url ?? "").ToLowerInvariant();
            return canonicalSport switch
            {
                "Calcio" => u.Contains("/scommesse/calcio"),
                "Pallacanestro" => u.Contains("/scommesse/pallacanestro"),
                "Tennis" => u.Contains("/scommesse/tennis"),
                "Rugby" => u.Contains("/scommesse/rugby"),
                _ => false
            };
        }

        private static async Task ClickSportSection(IPage page, string sportName)
        {
            if (UrlLooksLikeSport(page.Url, sportName))
            {
                Console.WriteLine($"Already on '{sportName}' page (URL check).");
                return;
            }

            Console.WriteLine($"Clicking sport: {sportName}");
            var links = page.Locator(".card.elemento-competizioni-widget .titolo-accordion a");
            int n = await links.CountAsync();

            // 1) exact text
            for (int i = 0; i < n; i++)
            {
                var link = links.Nth(i);
                string text = (await link.InnerTextAsync()).Trim();
                if (Canon(text) == Canon(sportName))
                {
                    await HandleRandomPopup(page);
                    await link.ClickAsync();
                    await page.WaitForTimeoutAsync(900);
                    return;
                }
            }

            // 2) contains
            for (int i = 0; i < n; i++)
            {
                var link = links.Nth(i);
                string text = (await link.InnerTextAsync()).Trim();
                if (Canon(text).Contains(Canon(sportName)))
                {
                    await HandleRandomPopup(page);
                    await link.ClickAsync();
                    await page.WaitForTimeoutAsync(900);
                    return;
                }
            }

            // 3) href fallback
            string[] keys = sportName switch
            {
                "Calcio" => CalcioHrefKeys,
                "Pallacanestro" => BasketHrefKeys,
                "Tennis" => TennisHrefKeys,
                "Rugby" => RugbyHrefKeys,
                _ => Array.Empty<string>()
            };
            for (int i = 0; i < n; i++)
            {
                var link = links.Nth(i);
                string? href = null;
                try { href = await link.GetAttributeAsync("href"); } catch { }
                href = (href ?? "").ToLowerInvariant();
                if (keys.Any(k => href.Contains(k)))
                {
                    await HandleRandomPopup(page);
                    await link.ClickAsync();
                    await page.WaitForTimeoutAsync(900);
                    return;
                }
            }

            Console.WriteLine($"Sport '{sportName}' not found among visible anchors.");
        }

        private static async Task<List<string>> GetLeaguesForSport(IPage page)
        {
            var leagueElements = page.Locator(".box-paese .nome-paese");
            int count = await leagueElements.CountAsync();
            var leagues = new List<string>();

            for (int i = 0; i < count; i++)
            {
                string leagueName = (await leagueElements.Nth(i).InnerTextAsync()).Trim();
                if (!string.IsNullOrEmpty(leagueName))
                    leagues.Add(leagueName);
            }

            return leagues;
        }

        // Rugby-only pause flag supported
        private static async Task ClickLeagueAndLoadOdds(IPage page, string leagueName, bool pauseAfterLoad = false)
        {
            Console.WriteLine($"Clicking '{leagueName}' and loading fixtures...");

            var leagueContainer = page.Locator(".box-paese", new PageLocatorOptions { HasTextString = leagueName }).First;

            if (await leagueContainer.IsVisibleAsync())
            {
                await HandleRandomPopup(page);

                var leagueButton = leagueContainer.Locator(".nome-paese");
                Console.WriteLine($"Clicking '{leagueName}'...");
                await leagueButton.ClickAsync();
                await page.WaitForTimeoutAsync(1000);

                var arrowButton = page.Locator(".widget-sel a:has(i.fas.fa-play)");

                if (await arrowButton.IsVisibleAsync())
                {
                    await HandleRandomPopup(page);

                    for (int i = 0; i < 10; i++)
                    {
                        var isDisabled = await arrowButton.GetAttributeAsync("href-disabled");
                        Console.WriteLine($"Arrow state: {(isDisabled == null ? "enabled" : "disabled")}");
                        if (isDisabled == null) break;
                        await page.WaitForTimeoutAsync(500);
                    }

                    await arrowButton.ScrollIntoViewIfNeededAsync();
                    await arrowButton.HoverAsync();
                    Console.WriteLine("Clicking ▶ arrow...");
                    await arrowButton.ClickAsync();

                    bool fixturesLoaded = false;
                    for (int i = 0; i < 10; i++)
                    {
                        var matchCount = await page.Locator(".tabellaQuoteSquadre").CountAsync();
                        if (matchCount > 0)
                        {
                            fixturesLoaded = true;
                            Console.WriteLine($"Fixtures loaded: {matchCount} matches found.");

                            if (pauseAfterLoad)
                            {
                                Console.WriteLine("Rugby fixtures loaded — pausing for manual inspection. Press ▶ in Playwright to continue.");
                                // await page.PauseAsync();
                            }
                            break;
                        }

                        Console.WriteLine("Fixtures not detected yet, retrying...");
                        await page.WaitForTimeoutAsync(2000);
                    }

                    if (!fixturesLoaded)
                        Console.WriteLine("⚠ Fixtures did not load after multiple retries, but continuing...");
                }
                else
                {
                    Console.WriteLine("▶ play-arrow not found inside widget-sel.");
                }
            }
            else
            {
                Console.WriteLine($"'{leagueName}' section not found!");
            }
        }

        public class MatchData
        {
            public string Teams { get; set; }
            public Dictionary<string, string> Odds { get; set; }
            public Dictionary<string, Dictionary<string, string>> TTPlusHandicap { get; set; }
            public Dictionary<string, Dictionary<string, string>> OUTotals { get; set; }
        }

        private static async Task<List<MatchData>> ExtractOddsAsync(IPage page, string fileName)
        {
            Console.WriteLine("Extracting odds...");

            var matchesList = new List<MatchData>();
            var matchContainers = page.Locator(".tabellaQuoteSquadre");
            int matchCount = await matchContainers.CountAsync();
            Console.WriteLine("Fixtures loading...");

            int validMatchCount = 0;

            for (int i = 0; i < matchCount; i++)
            {
                var match = matchContainers.Nth(i);

                // Scroll into view
                try
                {
                    await page.EvaluateAsync(@"(index) => {
                const container = document.querySelector('#primo-blocco-sport');
                const matches = container?.querySelectorAll('.tabellaQuoteSquadre');
                if (matches && matches[index]) {
                    matches[index].scrollIntoView({behavior: 'instant', block: 'center'});
                }
            }", i);
                    await page.WaitForTimeoutAsync(500);
                }
                catch { }

                // ---- Teams ----
                string teamNames = "Unknown Teams";
                var teams = new List<string>();
                try
                {
                    var bolds = await match.Locator("p.font-weight-bold").AllInnerTextsAsync();
                    foreach (var t in bolds)
                    {
                        var cleaned = (t ?? "").Replace('\u00A0', ' ').Trim();
                        if (string.IsNullOrWhiteSpace(cleaned)) continue;
                        var norm = Regex.Replace(cleaned, @"\s+", " ");
                        if (norm.Equals("vs", StringComparison.OrdinalIgnoreCase)) continue;
                        teams.Add(norm);
                        if (teams.Count == 2) break;
                    }
                }
                catch { }

                if (teams.Count < 2)
                {
                    try
                    {
                        var a = match.Locator("a");
                        if (await a.CountAsync() > 0)
                        {
                            var href = await a.First.GetAttributeAsync("href");
                            if (!string.IsNullOrWhiteSpace(href))
                            {
                                var last = (href.Split('/').LastOrDefault() ?? "").Replace("-", " ");
                                var parts = Regex.Split(last, @"\bvs\b", RegexOptions.IgnoreCase)
                                                 .Select(p => Regex.Replace(p.Trim(), @"\s+", " "))
                                                 .Where(p => !string.IsNullOrWhiteSpace(p))
                                                 .ToList();
                                if (parts.Count >= 2)
                                    teams = new List<string> { parts[0], parts[1] };
                            }
                        }
                    }
                    catch { }
                }

                if (teams.Count != 2) continue;
                teamNames = $"{teams[0]} vs {teams[1]}";
                validMatchCount++;
                Console.WriteLine($"\nProcessing match: {teamNames}");

                // ---- Base odds from THIS row’s grid ----
                var odds = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                bool isSoccer = fileName.Contains("Calcio", StringComparison.OrdinalIgnoreCase)
                             || fileName.Contains("Soccer", StringComparison.OrdinalIgnoreCase)
                             || fileName.Contains("Football", StringComparison.OrdinalIgnoreCase);

                bool isBasket = fileName.Contains("Pallacanestro", StringComparison.OrdinalIgnoreCase)
                             || fileName.Contains("Basket", StringComparison.OrdinalIgnoreCase);

                bool isTennis = fileName.Contains("Tennis", StringComparison.OrdinalIgnoreCase);

                bool isRugby = fileName.Contains("Rugby", StringComparison.OrdinalIgnoreCase);

                try
                {
                    var rowForOdds = match.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]").First;

                    var oddsElements = rowForOdds.Locator("div.gridInterernaQuotazioni > div.contenitoreSingolaQuota");
                    if (await oddsElements.CountAsync() == 0)
                        oddsElements = rowForOdds.Locator("div.tabellaQuoteNew > div.contenitoreSingolaQuota");

                    int oddsCount = await oddsElements.CountAsync();
                    for (int j = 0; j < oddsCount; j++)
                    {
                        var oddElement = oddsElements.Nth(j);
                        try
                        {
                            string label = (await oddElement.Locator(".titoloQuotazione").InnerTextAsync()).Trim();
                            string value = (await oddElement.Locator(".tipoQuotazione_1").InnerTextAsync()).Trim();
                            if (string.IsNullOrWhiteSpace(label) || string.IsNullOrWhiteSpace(value)) continue;

                            if (isRugby)
                            {
                                // Rugby base grid: 1, X, 2 only
                                if (label.Equals("1", StringComparison.OrdinalIgnoreCase) ||
                                    label.Equals("X", StringComparison.OrdinalIgnoreCase) ||
                                    label.Equals("2", StringComparison.OrdinalIgnoreCase))
                                {
                                    if (!odds.ContainsKey(label)) odds[label] = value;
                                }
                            }
                            else if (isSoccer)
                            {
                                // Soccer keeps everything visible in the GRID
                                odds[label] = value;
                            }
                            else
                            {
                                // Pallacanestro & Tennis: only the match-winner grid market "1" and/or "2"
                                if (label.Equals("1", StringComparison.OrdinalIgnoreCase) ||
                                    label.Equals("2", StringComparison.OrdinalIgnoreCase))
                                {
                                    if (!odds.ContainsKey(label)) odds[label] = value;
                                }
                            }
                        }
                        catch { }
                    }

                    if (isSoccer)
                    {
                        if (odds.TryGetValue("GOL", out var g) && !odds.ContainsKey("GOAL")) odds["GOAL"] = g;
                        if (odds.TryGetValue("NOGOL", out var ng) && !odds.ContainsKey("NOGOAL")) odds["NOGOAL"] = ng;
                    }
                }
                catch { }

                if (isSoccer)
                {
                    string[] wanted = { "1", "X", "2", "1X", "12", "X2", "UNDER", "OVER", "GOAL", "NOGOAL", "GOL", "NOGOL" };

                    try
                    {
                        var need = wanted.Where(k => !odds.ContainsKey(k)).ToArray();
                        if (need.Length > 0)
                        {
                            var rowForOdds = match.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]").First;
                            string json = await rowForOdds.EvaluateAsync<string>(@"(root, wanted) => {
                        const WANT = new Set((wanted||[]).map(s => (s||'').toString().trim().toUpperCase()));
                        const out = {};
                        const cells = root.querySelectorAll('div.tabellaQuoteNew .contenitoreSingolaQuota');
                        for (const cell of cells) {
                            const k = (cell.querySelector('.titoloQuotazione')?.textContent || '').trim().toUpperCase();
                            const v = (cell.querySelector('.tipoQuotazione_1')?.textContent || '').trim();
                            if (!k || !v) continue;
                            if (WANT.has(k)) out[k] = v;
                        }
                        return JSON.stringify(out);
                    }", need);

                            using var doc = JsonDocument.Parse(json);
                            foreach (var kv in doc.RootElement.EnumerateObject())
                            {
                                var key = kv.Name;
                                var val = kv.Value.GetString() ?? "";
                                if (!string.IsNullOrWhiteSpace(val) && !odds.ContainsKey(key))
                                    odds[key] = val;
                            }
                        }

                        if (odds.TryGetValue("GOL", out var g) && !odds.ContainsKey("GOAL")) odds["GOAL"] = g;
                        if (odds.TryGetValue("NOGOL", out var ng) && !odds.ContainsKey("NOGOAL")) odds["NOGOAL"] = ng;
                    }
                    catch { }
                }

                // Containers (kept for non-Rugby sports; Rugby will leave them empty)
                var hcp = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
                var ou = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

                // Existing extractions for Tennis/Basket (unchanged)
                bool doTennisExtractions = isTennis;
                bool doBasketExtractions = isBasket;

                if (doTennisExtractions || doBasketExtractions)
                {
                    var row = match.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]").First;
                    var buttons = row.Locator("button.dropdown-toggle");
                    int btnCount = 0; try { btnCount = await buttons.CountAsync(); } catch { }

                    static async Task CloseMenuAsync(ILocator btn)
                    { try { await btn.ClickAsync(new() { Force = true, Timeout = 500 }); } catch { } }

                    // 1) FIRST BUTTON → expected O/U
                    if (btnCount >= 1)
                    {
                        var btnOU = buttons.Nth(0);
                        try
                        {
                            if (await btnOU.IsEnabledAsync(new() { Timeout = 500 }))
                            {
                                await btnOU.ScrollIntoViewIfNeededAsync();
                                await btnOU.ClickAsync(new() { Timeout = 1500 });

                                await page.WaitForSelectorAsync(".dropdown-menu.show .lista-quote",
                                    new() { State = WaitForSelectorState.Visible, Timeout = 1000 });
                                await page.WaitForTimeoutAsync(120);

                                var ouTuples = await ParseNearestOUTotalMenuAsync(page, btnOU);
                                if (ouTuples != null && ouTuples.Count > 0)
                                {
                                    foreach (var (raw, under, over) in ouTuples)
                                    {
                                        var line = NormalizeOUTotalLine(raw);
                                        if (string.IsNullOrWhiteSpace(line)) continue;
                                        var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                        if (!string.IsNullOrWhiteSpace(under)) sides["UNDER"] = under.Trim();
                                        if (!string.IsNullOrWhiteSpace(over)) sides["OVER"] = over.Trim();
                                        if (sides.Count > 0) ou[line] = sides;
                                    }
                                }
                                await CloseMenuAsync(btnOU);
                            }
                        }
                        catch { }
                    }

                    // 2) SECOND BUTTON → expected TT+
                    if (btnCount >= 2)
                    {
                        var btnTT = buttons.Nth(1);
                        try
                        {
                            if (await btnTT.IsEnabledAsync(new() { Timeout = 500 }))
                            {
                                await btnTT.ScrollIntoViewIfNeededAsync();
                                await btnTT.ClickAsync(new() { Timeout = 1500 });

                                await page.WaitForSelectorAsync(".dropdown-menu.show .lista-quote.colonne-3, .dropdown-menu.show .lista-quote",
                                    new() { State = WaitForSelectorState.Visible, Timeout = 1000 });
                                await page.WaitForTimeoutAsync(120);

                                var ttTuples = await ParseNearestTTMenuAsync(page, btnTT);
                                if (ttTuples != null && ttTuples.Count > 0)
                                {
                                    foreach (var (raw, one, two) in ttTuples)
                                    {
                                        var line = NormalizeHcpLine(raw);
                                        if (string.IsNullOrWhiteSpace(line)) continue;
                                        var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                        if (!string.IsNullOrWhiteSpace(one)) sides["1"] = one.Trim();
                                        if (!string.IsNullOrWhiteSpace(two)) sides["2"] = two.Trim();
                                        if (sides.Count > 0) hcp[line] = sides;
                                    }
                                }
                                await CloseMenuAsync(btnTT);
                            }
                        }
                        catch { }
                    }
                }

                // ---- Log & collect ----
                string baseOdds = odds.Count > 0 ? string.Join(" | ", odds.Select(o => $"{o.Key}: {o.Value}")) : "No odds available";
                string hcpSummary = BuildHcpSummary(hcp);
                string ouSummary = BuildOUSummary(ou);

                Console.WriteLine($"Match: {teamNames}");
                Console.WriteLine(
                    "Odds: " + baseOdds +
                    (string.IsNullOrWhiteSpace(ouSummary) ? "" : $" | O/U: {ouSummary}") +
                    (string.IsNullOrWhiteSpace(hcpSummary) ? " | T/T + Handicap: -" : $" | T/T + Handicap: {hcpSummary}")
                );

                matchesList.Add(new MatchData
                {
                    Teams = teamNames,
                    Odds = odds,
                    TTPlusHandicap = hcp,
                    OUTotals = ou
                });
            }

            Console.WriteLine($"\n Fixtures loaded: {validMatchCount} matches found.");

            // === Important: Rugby O/U & Handicap extraction intentionally NOT performed ===

            // Transform Marathonbet scrape → Eurobet-shaped JSON
            var transformed = matchesList.Select(m =>
            {
                bool isSoccer =
                    fileName.Contains("Calcio", StringComparison.OrdinalIgnoreCase) ||
                    fileName.Contains("Soccer", StringComparison.OrdinalIgnoreCase) ||
                    fileName.Contains("Football", StringComparison.OrdinalIgnoreCase);

                bool isBasket =
                    fileName.Contains("Pallacanestro", StringComparison.OrdinalIgnoreCase) ||
                    fileName.Contains("Basket", StringComparison.OrdinalIgnoreCase);

                bool isTennis =
                    fileName.Contains("Tennis", StringComparison.OrdinalIgnoreCase);

                bool isRugby =
                    fileName.Contains("Rugby", StringComparison.OrdinalIgnoreCase);

                var oddsOut = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

                foreach (var kv in m.Odds ?? Enumerable.Empty<KeyValuePair<string, string>>())
                {
                    var key = kv.Key?.Trim() ?? "";
                    var up = key.ToUpperInvariant();

                    if (up == "GOAL" || up == "GOL")
                    {
                        oddsOut["GG"] = kv.Value;
                    }
                    else if (up == "NOGOAL" || up == "NOGOL")
                    {
                        oddsOut["NG"] = kv.Value;
                    }
                    else if (up == "UNDER" || up == "OVER")
                    {
                        // handled below in nested "O/U"
                    }
                    else
                    {
                        oddsOut[key] = kv.Value;  // "1","X","2","1X","12","X2", etc.
                    }
                }

                // Nested O/U (if any present for non-Rugby flows)
                var ouOut = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

                foreach (var kv in m.OUTotals ?? new Dictionary<string, Dictionary<string, string>>())
                {
                    var inner = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    if (kv.Value.TryGetValue("UNDER", out var u)) inner["U"] = u;
                    if (kv.Value.TryGetValue("OVER", out var o)) inner["O"] = o;
                    if (inner.Count > 0) ouOut[kv.Key] = inner;
                }

                // Soccer fallback for flat UNDER/OVER at 2.5
                if (ouOut.Count == 0 && isSoccer)
                {
                    string? uFlat = null, oFlat = null;
                    m.Odds?.TryGetValue("UNDER", out uFlat);
                    if (string.IsNullOrWhiteSpace(uFlat)) m.Odds?.TryGetValue("Under", out uFlat);
                    m.Odds?.TryGetValue("OVER", out oFlat);
                    if (string.IsNullOrWhiteSpace(oFlat)) m.Odds?.TryGetValue("Over", out oFlat);

                    var inner = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    if (!string.IsNullOrWhiteSpace(uFlat)) inner["U"] = uFlat;
                    if (!string.IsNullOrWhiteSpace(oFlat)) inner["O"] = oFlat;
                    if (inner.Count > 0) ouOut["2.5"] = inner;
                }

                if (ouOut.Count > 0) oddsOut["O/U"] = ouOut;

                // Handicap (kept for other sports)
                var hOut = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                foreach (var kv in m.TTPlusHandicap ?? new Dictionary<string, Dictionary<string, string>>())
                {
                    var inner = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    if (kv.Value.TryGetValue("1", out var one)) inner["1"] = one;
                    if (kv.Value.TryGetValue("2", out var two)) inner["2"] = two;
                    if (inner.Count > 0) hOut[kv.Key] = inner;
                }
                if (hOut.Count > 0) oddsOut["1 2 + Handicap"] = hOut;

                var sportCode = isSoccer ? "soccer" :
                                isBasket ? "basket" :
                                isTennis ? "tennis" :
                                isRugby  ? "rugby"  : "other";

                return new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                {
                    ["sport"] = sportCode,
                    ["Teams"] = m.Teams,
                    ["Bookmaker"] = "Marathonbet",
                    ["Odds"] = oddsOut
                };
            }).ToList();

            var jsonOptions = new JsonSerializerOptions
            {
                WriteIndented = true,
                Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };

            // POST to DB
            int posted = 0, failed = 0;
            foreach (var payload in transformed)
            {
                string teams = payload.TryGetValue("Teams", out var tObj) ? Convert.ToString(tObj) ?? "?" : "?";
                var (ok, respBody) = await PostJsonWithRetryAsync(POST_ENDPOINT, payload);
                if (ok) posted++; else failed++;
                Console.WriteLine($"{(ok ? "✅" : "❌")} [Marathonbet] {teams} → {respBody}");
                await Task.Delay(250);
            }
            Console.WriteLine($"POST summary [Marathonbet]: {posted} ok, {failed} failed.");

            // keep your JSON file export
            string jsonOutput = JsonSerializer.Serialize(transformed, jsonOptions);
            await File.WriteAllTextAsync(fileName, jsonOutput);

            Console.WriteLine($"\n Odds exported to {fileName}");
            return matchesList;
        }

        // ====== O/U helpers used by non-Rugby sports (unchanged) ======
        private static async Task<List<(string line, string under, string over)>> ParseNearestOUTotalMenuAsync(
            IPage page, ILocator anyButtonInRow)
        {
            try { await page.WaitForSelectorAsync(".dropdown-menu.show", new() { State = WaitForSelectorState.Visible, Timeout = 800 }); } catch { }

            var btnHandle = await anyButtonInRow.ElementHandleAsync();
            if (btnHandle == null) return new();

            string json = await page.EvaluateAsync<string>(@"(button) => {
      const T = s => (s||'').toString().trim();
      const vis = el => {
        if (!el) return false;
        const s = getComputedStyle(el), r = el.getBoundingClientRect();
        return s.visibility !== 'hidden' && s.display !== 'none' && r.width > 0 && r.height > 0;
      };
      const center = el => { const r = el.getBoundingClientRect(); return {x:(r.left+r.right)/2, y:(r.top+r.bottom)/2}; };
      const btnC = center(button);

      const menus = Array.from(document.querySelectorAll('.dropdown-menu.show')).filter(vis);
      if (!menus.length) return '[]';
      let box = null, best = Infinity;
      for (const m of menus) {
        const c = center(m), d2 = (c.x - btnC.x)**2 + (c.y - btnC.y)**2;
        if (d2 < best) { best = d2; box = m; }
      }
      if (!box) return '[]';

      let lists = Array.from(box.querySelectorAll('.dropdown-content .lista-quote.colonne-3')).filter(vis);
      lists = lists.filter(l => l.querySelector('a.dropdown-item'));
      if (!lists.length) lists = Array.from(box.querySelectorAll('.lista-quote')).filter(l => vis(l) && l.querySelector('a.dropdown-item'));
      if (!lists.length) return '[]';

      let list = null, b = Infinity;
      for (const L of lists) {
        const c = center(L), d2 = (c.x - btnC.x)**2 + (c.y - btnC.y)**2;
        if (d2 < b) { b = d2; list = L; }
      }
      if (!list) return '[]';

      const kids = Array.from(list.children || []);
      const out = [];
      for (let i = 0; i < kids.length; i++) {
        const k = kids[i];
        if (!(k.matches && k.matches('a.dropdown-item'))) continue;
        const raw = T(k.textContent);

        let under = '', over = '', seen = 0;
        for (let j = i + 1; j < kids.length && seen < 2; j++) {
          const n = kids[j];
          if (!(n.matches && n.matches('.contenitoreSingolaQuota'))) continue;
          const lab = T(n.querySelector('.titoloQuotazione')?.textContent).toUpperCase();
          const val = T(n.querySelector('.tipoQuotazione_1')?.textContent);
          if (lab === 'UNDER' && !under) { under = val; seen++; }
          else if (lab === 'OVER' && !over)  { over  = val; seen++; }
        }
        if (raw && (under || over)) out.push([raw, under, over]);
      }
      return JSON.stringify(out);
    }", btnHandle);

            var lines = new List<(string, string, string)>();
            try
            {
                using var doc = JsonDocument.Parse(json);
                foreach (var item in doc.RootElement.EnumerateArray())
                {
                    var raw = item[0].GetString() ?? "";
                    var under = item[1].GetString() ?? "";
                    var over = item[2].GetString() ?? "";
                    if (!string.IsNullOrWhiteSpace(raw) && (!string.IsNullOrWhiteSpace(under) || !string.IsNullOrWhiteSpace(over)))
                        lines.Add((raw, under, over));
                }
            }
            catch { }
            return lines;
        }

        private static string BuildOUSummary(Dictionary<string, Dictionary<string, string>> ou)
        {
            if (ou == null || ou.Count == 0) return "";
            var parsed = new SortedDictionary<double, (string Under, string Over)>();
            var fallback = new SortedDictionary<string, (string Under, string Over)>(StringComparer.Ordinal);

            foreach (var (lineKey, sides) in ou)
            {
                var un = sides.TryGetValue("UNDER", out var u) ? u : null;
                var ov = sides.TryGetValue("OVER", out var v) ? v : null;

                if (double.TryParse(lineKey.Replace(',', '.'), NumberStyles.Float,
                    CultureInfo.InvariantCulture, out var lineNum))
                    parsed[lineNum] = (un, ov);
                else
                    fallback[lineKey] = (un, ov);
            }

            var parts = new List<string>();
            foreach (var kv in parsed)
                parts.Add($"{kv.Key:0.##} (UNDER: {kv.Value.Under ?? "-"}, OVER: {kv.Value.Over ?? "-"})");
            foreach (var kv in fallback)
                parts.Add($"{kv.Key} (UNDER: {kv.Value.Under ?? "-"}, OVER: {kv.Value.Over ?? "-"})");

            return string.Join(" | ", parts);
        }

        private static string NormalizeOUTotalLine(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return "";
            var t = s.Trim().Replace(",", ".").Replace('\u00A0', ' ');
            var m = Regex.Match(t, @"[+\-]?\d+(?:\.\d+)?");
            return m.Success ? m.Value : t;
        }

        private static async Task HandleRandomPopup(IPage page)
        {
            try
            {
                for (int retry = 0; retry < 5; retry++)
                {
                    var overlay = page.Locator(".kumulos-background-mask").First;
                    var prompt = page.Locator(".kumulos-prompt").First;

                    bool isVisible = false;
                    try { isVisible = await overlay.IsVisibleAsync(new() { Timeout = 500 }); } catch { }
                    if (!isVisible)
                    {
                        try { isVisible = await prompt.IsVisibleAsync(new() { Timeout = 500 }); } catch { }
                    }

                    if (!isVisible)
                        return;

                    Console.WriteLine("Popup detected. Attempting to dismiss...");

                    var dismissButton = page.GetByText("Non ora");
                    if (await dismissButton.IsVisibleAsync(new() { Timeout = 500 }))
                    {
                        await dismissButton.ClickAsync(new() { Force = true });
                        Console.WriteLine("Clicked 'Non ora' dismiss button.");
                    }
                    else
                    {
                        var promptButton = prompt.Locator("button").First;
                        if (await promptButton.IsVisibleAsync(new() { Timeout = 500 }))
                        {
                            await promptButton.ClickAsync(new() { Force = true });
                            Console.WriteLine("Clicked popup button.");
                        }
                        else
                        {
                            Console.WriteLine("No dismiss button found. Clicking overlay...");
                            await overlay.ClickAsync(new() { Force = true });
                        }
                    }

                    try { await overlay.WaitForAsync(new() { State = WaitForSelectorState.Detached, Timeout = 3000 }); } catch { }
                    try { await prompt.WaitForAsync(new() { State = WaitForSelectorState.Detached, Timeout = 3000 }); } catch { }

                    await page.WaitForTimeoutAsync(500);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Popup handler error: {ex.Message}");
            }
        }
    }

}
