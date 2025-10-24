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
            var sports = await GetAllowedSports(page);

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
                Console.WriteLine("No matching sports after filtering (Calcio/Pallacanestro/Tennis/Rugby/Football Americano/Baseball).");
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
                            var x when RugbySyn.Contains(x) => "https://www.marathonbet.it/scommesse/rugby",
                            var x when AmericanFootballSyn.Contains(x) => "https://www.marathonbet.it/scommesse/football-americano",
                            var x when BaseballSyn.Contains(x) => "https://www.marathonbet.it/scommesse/baseball",
                            var x when IceHockeySyn.Contains(x) => "https://www.marathonbet.it/scommesse/hockey-su-ghiaccio", // <-- ADD
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
    url.Contains("/rugby") ? "Rugby" :
    url.Contains("/football-americano") || url.Contains("/american-football") ? "Football Americano" :
    url.Contains("/baseball") ? "Baseball" :
    url.Contains("/hockey-su-ghiaccio") || url.Contains("/ice-hockey") || url.Contains("/hockey") ? "Hockey su ghiaccio" : // <-- ADD
    "Calcio"
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
                    await ClickLeagueAndLoadOdds(page, league, pauseAfterLoad: true);
                    await HandleRandomPopup(page);

                    var safeSport = MakeSafeFilePart(sport);
                    var safeLeague = MakeSafeFilePart(league).Replace(" ", "_");

                    try
                    {
                        var fileName = $"{safeSport}_{safeLeague}.json";
                        var isAF = sport.Equals("Football Americano", StringComparison.OrdinalIgnoreCase);
                        var isBB = sport.Equals("Baseball", StringComparison.OrdinalIgnoreCase);
                        var isHK = sport.Equals("Hockey su ghiaccio", StringComparison.OrdinalIgnoreCase); // <-- ADD

                        // Defer post/save for AF, Baseball, Hockey
                        var matches = await ExtractOddsAsync(page, fileName, deferPostAndSave: isAF || isBB || isHK); // <-- ADD isHK

                        if (isAF || isBB || isHK) // <-- ADD isHK
                        {
                            // Snap-to-top (keep your existing code)
                            try { /* ...existing snap-to-top block... */ } catch { }

                            try
                            {
                                if (isAF)
                                {
                                    await EnrichAmericanFootballHandicapAsync(page, matches);
                                    try { await EnrichAmericanFootballOUAsync(page, matches); } catch (Exception ex) { Console.WriteLine("[AF/O-U] Enrichment error: " + ex.Message); }
                                }
                                else if (isBB)
                                {
                                    await EnrichBaseballHandicapAsync(page, matches);
                                    await EnrichBaseballOUAsync(page, matches);   // <— ADD THIS
                                }

                                else // Hockey: first O/U, then Handicap
                                {
                                    await EnrichIceHockeyOUAsync(page, matches);         // <-- ADD (new method below)
                                    await EnrichIceHockeyHandicapAsync(page, matches);   // <-- ADD (new method below)
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[{(isAF ? "AF" : isBB ? "BB" : "HK")}] Enrichment error: " + ex.Message); // <-- updated label
                            }

                            await TransformAndExportAsync(fileName, matches, sport, league);
                        }

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

public static async Task RunSinglePageAsync(IPage page, string startSport, bool allowDirectFallback = false)
{
    // 1) Landing
    Console.WriteLine("Navigating to Marathonbet (single-page run)...");
    page.SetDefaultNavigationTimeout(90_000);

    await page.GotoAsync("https://www.marathonbet.it/scommesse",
        new() { WaitUntil = WaitUntilState.Load, Timeout = 90_000 });

    await HandleRandomPopup(page);
    await EnsureAllSportsVisibleAsync(page);

    // 2) Read visible sports and filter to requested one
    var sports = await GetAllowedSports(page);

    if (!string.IsNullOrWhiteSpace(startSport))
    {
        var wanted = CanonicalFromTextOrHref(startSport, null);
        if (wanted != null)
        {
            sports = sports.Where(s => s.Equals(wanted, StringComparison.OrdinalIgnoreCase)).ToList();
        }
        else
        {
            sports = new List<string>(); // force fallback if allowed
        }
    }

    // 3) Optional direct URL fallback when the sport isn’t visible in the sidebar
    if (sports.Count == 0 && allowDirectFallback && !string.IsNullOrWhiteSpace(startSport))
    {
        var s = Canon(startSport);
        string? url = s switch
        {
            var x when CalcioSyn.Contains(x)            => "https://www.marathonbet.it/scommesse/calcio",
            var x when BasketSyn.Contains(x)            => "https://www.marathonbet.it/scommesse/pallacanestro",
            var x when TennisSyn.Contains(x)            => "https://www.marathonbet.it/scommesse/tennis",
            var x when RugbySyn.Contains(x)             => "https://www.marathonbet.it/scommesse/rugby",
            var x when AmericanFootballSyn.Contains(x)  => "https://www.marathonbet.it/scommesse/football-americano",
            var x when BaseballSyn.Contains(x)          => "https://www.marathonbet.it/scommesse/baseball",
            var x when IceHockeySyn.Contains(x)         => "https://www.marathonbet.it/scommesse/hockey-su-ghiaccio",
            _ => null
        };

        if (url != null)
        {
            Console.WriteLine($"[Fallback] Navigating directly to: {url}");
            await page.GotoAsync(url, new() { WaitUntil = WaitUntilState.NetworkIdle, Timeout = 90_000 });
            await HandleRandomPopup(page);

            // normalize to one canonical sport name so downstream code works
            var canonical = url.Contains("/calcio") ? "Calcio" :
                            url.Contains("/pallacanestro") ? "Pallacanestro" :
                            url.Contains("/tennis") ? "Tennis" :
                            url.Contains("/rugby") ? "Rugby" :
                            (url.Contains("/football-americano") || url.Contains("/american-football")) ? "Football Americano" :
                            url.Contains("/baseball") ? "Baseball" :
                            (url.Contains("/hockey-su-ghiaccio") || url.Contains("/ice-hockey") || url.Contains("/hockey")) ? "Hockey su ghiaccio" :
                            null;

            if (canonical != null) sports = new List<string> { canonical };
        }
    }

    if (sports.Count == 0)
    {
        Console.WriteLine($"No visible/fallback sport matched '{startSport}'. Stopping single-page run.");
        return;
    }

    Console.WriteLine($"[SinglePage] Sports to process: {string.Join(", ", sports)}");

    // 4) Scrape each requested sport using the SAME page
    foreach (var sport in sports)
    {
        Console.WriteLine($"\n=== Processing sport: {sport} ===");

        // Go to sport by visible click if possible (safer than direct URL)
        await ClickSportSection(page, sport);
        await HandleRandomPopup(page);

        var leagues = await GetLeaguesForSport(page);

        foreach (var league in leagues)
        {
            Console.WriteLine($"\n--- Processing league: {league} ---");

            await HandleRandomPopup(page);
            await ClickLeagueAndLoadOdds(page, league, pauseAfterLoad: true);
            await HandleRandomPopup(page);

            var safeSport  = MakeSafeFilePart(sport);
            var safeLeague = MakeSafeFilePart(league).Replace(" ", "_");
            var fileName   = $"{safeSport}_{safeLeague}.json";

            // Defer POST/save for these sports until after enrichment
            bool isAF = sport.Equals("Football Americano", StringComparison.OrdinalIgnoreCase);
            bool isBB = sport.Equals("Baseball", StringComparison.OrdinalIgnoreCase);
            bool isHK = sport.Equals("Hockey su ghiaccio", StringComparison.OrdinalIgnoreCase);

            // 5) Extract base odds
            var matches = await ExtractOddsAsync(page, fileName, deferPostAndSave: isAF || isBB || isHK);

            // 6) Optional enrichment by sport
            if (isAF || isBB || isHK)
            {
                try
                {
                    if (isAF)
                    {
                        await EnrichAmericanFootballHandicapAsync(page, matches);
                        try { await EnrichAmericanFootballOUAsync(page, matches); } catch (Exception ex) { Console.WriteLine("[AF/O-U] " + ex.Message); }
                    }
                    else if (isBB)
                    {
                        await EnrichBaseballHandicapAsync(page, matches);
                        await EnrichBaseballOUAsync(page, matches);
                    }
                    else // Hockey
                    {
                        await EnrichIceHockeyOUAsync(page, matches);
                        await EnrichIceHockeyHandicapAsync(page, matches);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Enrichment error] {sport}: {ex.Message}");
                }

                // 7) Transform + POST + export JSON (only for deferred sports)
                await TransformAndExportAsync(fileName, matches, sport, league);
            }

            // 8) Save to DB (all sports)
            try
            {
                await DbSaver.SaveOddsAsync("marathonbet", sport, league, matches);
                Console.WriteLine($"Inserted {matches.Count} matches into DB.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("DB save failed: " + ex.Message);
            }

            // 9) Reset back to sport landing for the next league (same page)
            await page.GotoAsync("https://www.marathonbet.it/scommesse",
                new() { WaitUntil = WaitUntilState.NetworkIdle, Timeout = 90_000 });

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

                    // O/U
                    foreach (var (line, sides) in m.OUTotals)
                    {
                        foreach (var pick in new[] { "UNDER", "OVER" })
                        {
                            if (sides.TryGetValue(pick, out var val))
                                await InsertAsync(conn, sql, bookmaker, sport, league, home, away,
                                    "OU", line, pick, val);
                        }
                    }

                    // TT Handicap
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
        // Models
        // =========================
        public class MatchData
        {
            public string Teams { get; set; }
            public Dictionary<string, string> Odds { get; set; } = new(StringComparer.OrdinalIgnoreCase);
            public Dictionary<string, Dictionary<string, string>> TTPlusHandicap { get; set; } = new(StringComparer.OrdinalIgnoreCase);
            public Dictionary<string, Dictionary<string, string>> OUTotals { get; set; } = new(StringComparer.OrdinalIgnoreCase);
        }

        // =========================
        // Transform+Export (used only when AF/BB defers post)
        // =========================
        private static async Task TransformAndExportAsync(
      string fileName,
      List<MatchData> matches,
      string sport,
      string league)
        {
            var transformed = matches.Select(m =>
            {
                bool isIceHockey = fileName.Contains("Hockey su ghiaccio", StringComparison.OrdinalIgnoreCase)
                                || fileName.Contains("Ice Hockey", StringComparison.OrdinalIgnoreCase)
                                || fileName.Contains("Ice-Hockey", StringComparison.OrdinalIgnoreCase);

                bool isAmericanFootball = fileName.Contains("Football Americano", StringComparison.OrdinalIgnoreCase) ||
                                          fileName.Contains("American Football", StringComparison.OrdinalIgnoreCase);
                bool isBaseball = fileName.Contains("Baseball", StringComparison.OrdinalIgnoreCase);

                bool isSoccer = fileName.Contains("Calcio", StringComparison.OrdinalIgnoreCase) ||
                                fileName.Contains("Soccer", StringComparison.OrdinalIgnoreCase);
                bool isBasket = fileName.Contains("Pallacanestro", StringComparison.OrdinalIgnoreCase) ||
                                fileName.Contains("Basket", StringComparison.OrdinalIgnoreCase);
                bool isTennis = fileName.Contains("Tennis", StringComparison.OrdinalIgnoreCase);
                bool isRugby = fileName.Contains("Rugby", StringComparison.OrdinalIgnoreCase);

                var oddsOut = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

                static bool TryNum(string s, out decimal d) =>
                    decimal.TryParse((s ?? "").Replace(',', '.'), NumberStyles.Float, CultureInfo.InvariantCulture, out d);
                static object NumOrRawAF(string s) => TryNum(s, out var d) ? d : s;

                // Base odds
                foreach (var kv in m.Odds ?? Enumerable.Empty<KeyValuePair<string, string>>())
                {
                    var key = kv.Key?.Trim() ?? "";
                    var up = key.ToUpperInvariant();

                    if (up == "UNDER" || up == "OVER") continue;

                    if (isAmericanFootball) // AF numeric normalization preserved
                    {
                        oddsOut[key] = NumOrRawAF(kv.Value);
                    }
                    else if (up == "GOAL" || up == "GOL")
                    {
                        oddsOut["GG"] = kv.Value;
                    }
                    else if (up == "NOGOAL" || up == "NOGOL")
                    {
                        oddsOut["NG"] = kv.Value;
                    }
                    else
                    {
                        oddsOut[key] = kv.Value;
                    }
                }

                // --------- O/U section (Prototype-A-compatible for Basketball) ---------
                // Build nested O/U from OUTotals (lines -> { U, O })
                var ouOut = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                foreach (var kv in m.OUTotals ?? new())
                {
                    var inner = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    if (kv.Value.TryGetValue("UNDER", out var u)) inner["U"] = u;
                    if (kv.Value.TryGetValue("OVER", out var o)) inner["O"] = o;
                    if (inner.Count > 0) ouOut[kv.Key] = inner;
                }

                if (isBasket || isBaseball)   // <— include baseball here
                {
                    if (ouOut.Count > 0) oddsOut["O/U"] = ouOut;
                }
                else
                {
                    var (uBest, oBest) = PickBestOU(m.OUTotals);
                    if (uBest != null) oddsOut["UNDER"] = isAmericanFootball ? NumOrRawAF(uBest) : uBest;
                    if (oBest != null) oddsOut["OVER"] = isAmericanFootball ? NumOrRawAF(oBest) : oBest;

                    // soccer-only fallback remains the same
                    if (!oddsOut.ContainsKey("UNDER") && !oddsOut.ContainsKey("OVER") && isSoccer)
                    {
                        string? uFlat = null, oFlat = null;
                        m.Odds?.TryGetValue("UNDER", out uFlat);
                        if (string.IsNullOrWhiteSpace(uFlat)) m.Odds?.TryGetValue("Under", out uFlat);
                        m.Odds?.TryGetValue("OVER", out oFlat);
                        if (string.IsNullOrWhiteSpace(oFlat)) m.Odds?.TryGetValue("Over", out oFlat);
                        if (!string.IsNullOrWhiteSpace(uFlat)) oddsOut["UNDER"] = isAmericanFootball ? NumOrRawAF(uFlat) : uFlat;
                        if (!string.IsNullOrWhiteSpace(oFlat)) oddsOut["OVER"] = isAmericanFootball ? NumOrRawAF(oFlat) : oFlat;
                    }
                }

                {
                    // keep B's existing flat UNDER/OVER behavior (AF numeric normalization included)
                    var (uBest, oBest) = PickBestOU(m.OUTotals);
                    if (uBest != null) oddsOut["UNDER"] = isAmericanFootball ? NumOrRawAF(uBest) : uBest;
                    if (oBest != null) oddsOut["OVER"] = isAmericanFootball ? NumOrRawAF(oBest) : oBest;

                    // soccer-only fallback (unchanged)
                    if (!oddsOut.ContainsKey("UNDER") && !oddsOut.ContainsKey("OVER") && isSoccer)
                    {
                        string? uFlat = null, oFlat = null;
                        m.Odds?.TryGetValue("UNDER", out uFlat);
                        if (string.IsNullOrWhiteSpace(uFlat)) m.Odds?.TryGetValue("Under", out uFlat);
                        m.Odds?.TryGetValue("OVER", out oFlat);
                        if (string.IsNullOrWhiteSpace(oFlat)) m.Odds?.TryGetValue("Over", out oFlat);
                        if (!string.IsNullOrWhiteSpace(uFlat)) oddsOut["UNDER"] = isAmericanFootball ? NumOrRawAF(uFlat) : uFlat;
                        if (!string.IsNullOrWhiteSpace(oFlat)) oddsOut["OVER"] = isAmericanFootball ? NumOrRawAF(oFlat) : oFlat;
                    }
                }
                // -----------------------------------------------------------------------

                // Handicap (1 2 + Handicap)
                var hOut = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                foreach (var kv in m.TTPlusHandicap ?? new())
                {
                    var inner = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                    if (kv.Value.TryGetValue("1", out var one)) inner["1"] = isAmericanFootball ? NumOrRawAF(one) : (object)one;
                    if (kv.Value.TryGetValue("2", out var two)) inner["2"] = isAmericanFootball ? NumOrRawAF(two) : (object)two;
                    if (inner.Count > 0) hOut[kv.Key] = inner;
                }
                if (hOut.Count > 0) oddsOut["1 2 + Handicap"] = hOut;

                var sportCode = isAmericanFootball ? "americanfootball" :
                 isBaseball ? "baseball" :
                 isSoccer ? "soccer" :
                 isBasket ? "basket" :
                 isTennis ? "tennis" :
                 isRugby ? "rugby" :
                 isIceHockey ? "hockey" : "other";

                return new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                {
                    ["sport"] = sportCode,
                    ["Teams"] = m.Teams,
                    ["Bookmaker"] = "Marathonbet",
                    ["Odds"] = oddsOut
                };
            }).ToList();

            // POST
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

            // Save JSON
            var jsonOptions = new JsonSerializerOptions
            {
                WriteIndented = true,
                Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };
            string jsonOutput = JsonSerializer.Serialize(transformed, jsonOptions);
            await File.WriteAllTextAsync(fileName, jsonOutput);

            Console.WriteLine($"\n Odds exported to {fileName}");
        }

        // =========================
        // Extract grid odds (baseline)
        // =========================
        private static async Task<List<MatchData>> ExtractOddsAsync(IPage page, string fileName, bool deferPostAndSave = false)
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

                bool isAmericanFootball = fileName.Contains("Football Americano", StringComparison.OrdinalIgnoreCase)
                                       || fileName.Contains("American Football", StringComparison.OrdinalIgnoreCase);
                bool isBaseball = fileName.Contains("Baseball", StringComparison.OrdinalIgnoreCase);

                bool isSoccer = fileName.Contains("Calcio", StringComparison.OrdinalIgnoreCase)
                             || fileName.Contains("Soccer", StringComparison.OrdinalIgnoreCase);
                bool isBasket = fileName.Contains("Pallacanestro", StringComparison.OrdinalIgnoreCase)
                             || fileName.Contains("Basket", StringComparison.OrdinalIgnoreCase);
                bool isTennis = fileName.Contains("Tennis", StringComparison.OrdinalIgnoreCase);
                bool isRugby = fileName.Contains("Rugby", StringComparison.OrdinalIgnoreCase);
                bool isHockey = fileName.Contains("Hockey su ghiaccio", StringComparison.OrdinalIgnoreCase)
                             || fileName.Contains("Ice Hockey", StringComparison.OrdinalIgnoreCase)
                             || fileName.Contains("Ice-Hockey", StringComparison.OrdinalIgnoreCase);

                try
                {
                    var rowForOdds = match.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]").First;

                    // Keep row centered ONLY for AF (others untouched)
                    if (isAmericanFootball)
                        await KeepInViewAsync(page, rowForOdds);

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
                                if (label.Equals("1", StringComparison.OrdinalIgnoreCase) ||
                                    label.Equals("X", StringComparison.OrdinalIgnoreCase) ||
                                    label.Equals("2", StringComparison.OrdinalIgnoreCase))
                                {
                                    if (!odds.ContainsKey(label)) odds[label] = value;
                                }
                            }
                            else if (isSoccer)
                            {
                                odds[label] = value;
                            }
                            else if (isAmericanFootball)
                            {
                                if (label.Equals("1", StringComparison.OrdinalIgnoreCase) ||
                                    label.Equals("X", StringComparison.OrdinalIgnoreCase) ||
                                    label.Equals("2", StringComparison.OrdinalIgnoreCase))
                                {
                                    if (!odds.ContainsKey(label)) odds[label] = value;
                                }
                            }
                            else if (isBaseball)
                            {
                                if (label.Equals("1", StringComparison.OrdinalIgnoreCase) ||
                                    label.Equals("2", StringComparison.OrdinalIgnoreCase))
                                {
                                    if (!odds.ContainsKey(label)) odds[label] = value;
                                }
                            }
                            else if (isHockey)
                            {
                                if (label.Equals("1", StringComparison.OrdinalIgnoreCase) ||
                                    label.Equals("2", StringComparison.OrdinalIgnoreCase))
                                {
                                    if (!odds.ContainsKey(label)) odds[label] = value;
                                }
                            }
                            else
                            {
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

                // Containers for optional nested markets
                var hcp = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
                var ou = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

                // (Existing extractions for Tennis/Basket unchanged)
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
                                if (ttTuples is { Count: > 0 })
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

            // Transform Marathonbet scrape → JSON
            var transformed = matchesList.Select(m =>
            {
                bool isAmericanFootball =
                    fileName.Contains("Football Americano", StringComparison.OrdinalIgnoreCase) ||
                    fileName.Contains("American Football", StringComparison.OrdinalIgnoreCase);
                bool isBaseball =
                    fileName.Contains("Baseball", StringComparison.OrdinalIgnoreCase);

                bool isSoccer =
                    fileName.Contains("Calcio", StringComparison.OrdinalIgnoreCase) ||
                    fileName.Contains("Soccer", StringComparison.OrdinalIgnoreCase);
                bool isBasket =
                    fileName.Contains("Pallacanestro", StringComparison.OrdinalIgnoreCase) ||
                    fileName.Contains("Basket", StringComparison.OrdinalIgnoreCase);
                bool isTennis =
                    fileName.Contains("Tennis", StringComparison.OrdinalIgnoreCase);
                bool isRugby =
                    fileName.Contains("Rugby", StringComparison.OrdinalIgnoreCase);
                bool isIceHockey = fileName.Contains("Hockey su ghiaccio", StringComparison.OrdinalIgnoreCase)
                || fileName.Contains("Ice Hockey", StringComparison.OrdinalIgnoreCase)
                || fileName.Contains("Ice-Hockey", StringComparison.OrdinalIgnoreCase);

                var oddsOut = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

                static bool TryNum(string s, out decimal d) =>
                    decimal.TryParse((s ?? "").Replace(',', '.'), NumberStyles.Float, CultureInfo.InvariantCulture, out d);
                static object NumOrRawAF(string s) =>
                    TryNum(s, out var d) ? d : s;

                // Base grid odds
                foreach (var kv in m.Odds ?? Enumerable.Empty<KeyValuePair<string, string>>())
                {
                    var key = kv.Key?.Trim() ?? "";
                    var up = key.ToUpperInvariant();

                    if (up == "UNDER" || up == "OVER")
                    {
                        continue;
                    }

                    if (isAmericanFootball)
                    {
                        oddsOut[key] = NumOrRawAF(kv.Value);
                    }
                    else if (up == "GOAL" || up == "GOL")
                    {
                        oddsOut["GG"] = kv.Value;
                    }
                    else if (up == "NOGOAL" || up == "NOGOL")
                    {
                        oddsOut["NG"] = kv.Value;
                    }
                    else
                    {
                        oddsOut[key] = kv.Value;  // 1, X, 2, etc.
                    }
                }

                // --------- O/U section (Prototype-A-compatible for Basketball) ---------
                var ouOut = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                foreach (var kv in m.OUTotals ?? new Dictionary<string, Dictionary<string, string>>())
                {
                    var inner = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    if (kv.Value.TryGetValue("UNDER", out var u)) inner["U"] = u;
                    if (kv.Value.TryGetValue("OVER", out var o)) inner["O"] = o;
                    if (inner.Count > 0) ouOut[kv.Key] = inner;
                }

                if (isBasket)
                {
                    // EXACTLY like Prototype A: only nested "O/U" for basketball
                    if (ouOut.Count > 0) oddsOut["O/U"] = ouOut;
                }
                else
                {
                    // keep existing flat UNDER/OVER for others (AF normalization included)
                    var (uBest, oBest) = PickBestOU(m.OUTotals);
                    if (uBest != null) oddsOut["UNDER"] = isAmericanFootball ? NumOrRawAF(uBest) : uBest;
                    if (oBest != null) oddsOut["OVER"] = isAmericanFootball ? NumOrRawAF(oBest) : oBest;

                    // soccer-only fallback (unchanged)
                    if (!oddsOut.ContainsKey("UNDER") && !oddsOut.ContainsKey("OVER") && isSoccer)
                    {
                        string? uFlat = null, oFlat = null;
                        m.Odds?.TryGetValue("UNDER", out uFlat);
                        if (string.IsNullOrWhiteSpace(uFlat)) m.Odds?.TryGetValue("Under", out uFlat);
                        m.Odds?.TryGetValue("OVER", out oFlat);
                        if (string.IsNullOrWhiteSpace(oFlat)) m.Odds?.TryGetValue("Over", out oFlat);
                        if (!string.IsNullOrWhiteSpace(uFlat)) oddsOut["UNDER"] = isAmericanFootball ? NumOrRawAF(uFlat) : uFlat;
                        if (!string.IsNullOrWhiteSpace(oFlat)) oddsOut["OVER"] = isAmericanFootball ? NumOrRawAF(oFlat) : oFlat;
                    }
                }
                // -----------------------------------------------------------------------

                // Handicap (1 2 + Handicap)
                var hOut = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                foreach (var kv in m.TTPlusHandicap ?? new Dictionary<string, Dictionary<string, string>>())
                {
                    var inner = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                    if (kv.Value.TryGetValue("1", out var one)) inner["1"] = isAmericanFootball ? NumOrRawAF(one) : (object)one;
                    if (kv.Value.TryGetValue("2", out var two)) inner["2"] = isAmericanFootball ? NumOrRawAF(two) : (object)two;
                    if (inner.Count > 0) hOut[kv.Key] = inner;
                }
                if (hOut.Count > 0) oddsOut["1 2 + Handicap"] = hOut;

                var sportCode = isAmericanFootball ? "americanfootball" :
                 isBaseball ? "baseball" :
                 isSoccer ? "soccer" :
                 isBasket ? "basket" :
                 isTennis ? "tennis" :
                 isRugby ? "rugby" :
                 isIceHockey ? "hockey" : "other";

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

            // For AF & Baseball (and Hockey if you passed deferPostAndSave) we defer post/save until after enrichment.
            if (!deferPostAndSave)
            {
                // POST to endpoint
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
            }

            return matchesList;
        }

        // ====== O/U helpers (also reused) ======
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


        private static async Task<(string line, string one, string two)?> ExtractInlineHcpNoDropdownAsync(ILocator rowContainer)
        {
            try
            {
                // Presence of the button without arrow means the selected handicap is inline
                var btnNoArrow = rowContainer.Locator(".gridInterernaQuotazioni > button.dropdown-toggle-no-arrow");
                if (!await btnNoArrow.IsVisibleAsync(new() { Timeout = 400 })) return null;

                // Line text: inside the span.uo-selezionato (e.g., "-5.5")
                var lineSpan = btnNoArrow.Locator("span.uo-selezionato");
                if (!await lineSpan.IsVisibleAsync(new() { Timeout = 400 })) return null;

                var rawLine = (await lineSpan.InnerTextAsync())?.Trim() ?? "";
                var line = NormalizeHcpLine(rawLine);
                if (string.IsNullOrWhiteSpace(line)) return null;

                // Visible 1/2 odds cells within the same grid
                var cells = rowContainer.Locator(".gridInterernaQuotazioni .contenitoreSingolaQuota");
                int count = 0; try { count = await cells.CountAsync(); } catch { }
                if (count == 0) return null;

                string one = "", two = "";
                for (int i = 0; i < count; i++)
                {
                    try
                    {
                        var cell = cells.Nth(i);
                        var lab = ((await cell.Locator(".titoloQuotazione").InnerTextAsync()) ?? "").Trim().ToUpperInvariant();
                        var val = ((await cell.Locator(".tipoQuotazione_1").InnerTextAsync()) ?? "").Trim();
                        if (lab == "1" && string.IsNullOrEmpty(one)) one = val;
                        else if (lab == "2" && string.IsNullOrEmpty(two)) two = val;
                    }
                    catch { /* keep going */ }
                }

                if (string.IsNullOrWhiteSpace(one) && string.IsNullOrWhiteSpace(two)) return null;
                return (line, one, two);
            }
            catch { return null; }
        }


        // =========================
        // Rugby: Filter click + enrich every row with O/U (UNCHANGED)
        // =========================
        private static async Task EnrichRugbyOUAsync(IPage page, List<MatchData> matches)
        {
            if (matches == null || matches.Count == 0) return;

            static string ToUO(string s)
            {
                var t = (s ?? "").Trim().ToUpperInvariant();
                if (t == "U" || t.Contains("UNDER") || t.Contains("SOTTO")) return "UNDER";
                if (t == "O" || t.Contains("OVER") || t.Contains("SOPRA")) return "OVER";
                return t;
            }

            bool clickedFilter = false;
            var ouTile = page.Locator("div.filter.Zprincipali[data-id='a-435'] >> text=Under/Over");
            if (await ouTile.IsVisibleAsync(new() { Timeout = 700 }))
            {
                try { await ouTile.ClickAsync(); clickedFilter = true; } catch { }
            }

            if (!clickedFilter)
            {
                var alt = page.GetByText("Under/Over").Locator("xpath=ancestor::div[contains(@class,'filter') and contains(@class,'Zprincipali')]");
                if (await alt.IsVisibleAsync(new() { Timeout = 700 }))
                {
                    try { await alt.ClickAsync(); clickedFilter = true; } catch { }
                }
            }

            if (!clickedFilter)
            {
                Console.WriteLine("[Rugby] No Under/Over filter found for this league — skipping O/U.");
                return;
            }

            var byTeams = new Dictionary<string, MatchData>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in matches)
            {
                var k = (m?.Teams ?? "").Replace('\u00A0', ' ').Trim();
                if (!string.IsNullOrWhiteSpace(k) && !byTeams.ContainsKey(k))
                    byTeams[k] = m;
                if (m.OUTotals == null)
                    m.OUTotals = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            }

            var rows = page.Locator(".tabellaQuoteSquadre");
            int rowCount = 0; try { rowCount = await rows.CountAsync(); } catch { }
            if (rowCount == 0) return;

            for (int i = 0; i < rowCount; i++)
            {
                var row = rows.Nth(i);

                string keyTeams = "";
                try
                {
                    var names = await row.Locator("p.font-weight-bold").AllInnerTextsAsync();
                    var list = names.Select(t => Regex.Replace((t ?? "").Replace('\u00A0', ' ').Trim(), @"\s+", " "))
                                    .Where(t => !string.Equals(t, "vs", StringComparison.OrdinalIgnoreCase))
                                    .Take(2)
                                    .ToList();
                    if (list.Count == 2) keyTeams = $"{list[0]} vs {list[1]}";
                }
                catch { }

                if (string.IsNullOrWhiteSpace(keyTeams) || !byTeams.TryGetValue(keyTeams, out var match))
                    continue;

                var rowContainer = row.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]").First;

                // Selected line odds in the row (if visible)
                try
                {
                    var selectedSpan = rowContainer.Locator("span.uo-selezionato").First;
                    if (await selectedSpan.IsVisibleAsync(new() { Timeout = 400 }))
                    {
                        var sel = (await selectedSpan.InnerTextAsync())?.Trim() ?? "";
                        var line = NormalizeOUTotalLine(sel);
                        if (!string.IsNullOrWhiteSpace(line))
                        {
                            var cellBlocks = rowContainer.Locator(".contenitoreSingolaQuota");
                            int cCount = 0; try { cCount = await cellBlocks.CountAsync(); } catch { }

                            string uVal = "", oVal = "";
                            for (int c = 0; c < cCount; c++)
                            {
                                try
                                {
                                    var block = cellBlocks.Nth(c);
                                    var lab = ToUO(await block.Locator(".titoloQuotazione").InnerTextAsync());
                                    var val = (await block.Locator(".tipoQuotazione_1").InnerTextAsync())?.Trim() ?? "";
                                    if (lab == "UNDER" && string.IsNullOrEmpty(uVal)) uVal = val;
                                    else if (lab == "OVER" && string.IsNullOrEmpty(oVal)) oVal = val;
                                }
                                catch { }
                            }

                            if (!string.IsNullOrWhiteSpace(uVal) || !string.IsNullOrWhiteSpace(oVal))
                            {
                                var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                if (!string.IsNullOrWhiteSpace(uVal)) sides["UNDER"] = uVal;
                                if (!string.IsNullOrWhiteSpace(oVal)) sides["OVER"] = oVal;
                                if (sides.Count > 0) match.OUTotals[line] = sides;
                            }
                        }
                    }
                }
                catch { }

                // Open dropdown and iterate all O/U lines
                var btnOU = rowContainer.Locator("button.dropdown-toggle:has(span.uo-selezionato)").First;
                if (!await btnOU.IsVisibleAsync(new() { Timeout = 500 })) continue;

                try
                {
                    await btnOU.ScrollIntoViewIfNeededAsync();
                    await btnOU.ClickAsync(new() { Timeout = 1500 });

                    try
                    {
                        await page.WaitForSelectorAsync(".dropdown-menu.show", new() { State = WaitForSelectorState.Visible, Timeout = 1000 });
                        await page.WaitForTimeoutAsync(120);
                    }
                    catch { }

                    var tuples = await ParseNearestOUTotalMenuAsync(page, btnOU);
                    if (tuples != null && tuples.Count > 0)
                    {
                        foreach (var (raw, under, over) in tuples)
                        {
                            var line = NormalizeOUTotalLine(raw);
                            if (string.IsNullOrWhiteSpace(line)) continue;

                            var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                            if (!string.IsNullOrWhiteSpace(under)) sides["UNDER"] = under.Trim();
                            if (!string.IsNullOrWhiteSpace(over)) sides["OVER"] = over.Trim();

                            if (sides.Count > 0)
                                match.OUTotals[line] = sides;
                        }
                    }
                }
                catch { }
                finally
                {
                    try { await btnOU.ClickAsync(new() { Timeout = 500, Force = true }); } catch { }
                }
            }

            Console.WriteLine("[Rugby] O/U enrichment done.");
        }

        // =========================
        // BASEBALL: Under/Over enrichment (collect all lines per row)
        // =========================
        private static async Task EnrichBaseballOUAsync(IPage page, List<MatchData> matches)
        {
            if (matches == null || matches.Count == 0) return;

            // Open the global Under/Over filter if present
            try
            {
                await OpenOUFilterFromFirstFixtureAsync(page);
                var opened = await ClickVisibleOUFilterAsync(page);
                if (!opened)
                {
                    Console.WriteLine("[BB/O-U] Under/Over filter not found — skipping.");
                    return;
                }
            }
            catch
            {
                Console.WriteLine("[BB/O-U] Error opening O/U macro — skipping.");
                return;
            }

            await HandleRandomPopup(page);
            await page.WaitForTimeoutAsync(150);

            var byTeams = new Dictionary<string, MatchData>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in matches)
            {
                var k = (m?.Teams ?? "").Replace('\u00A0', ' ').Trim();
                if (!string.IsNullOrWhiteSpace(k) && !byTeams.ContainsKey(k)) byTeams[k] = m;
                m.OUTotals ??= new(StringComparer.OrdinalIgnoreCase);
            }

            var rows = page.Locator(".tabellaQuoteSquadre");
            int rowCount = 0; try { rowCount = await rows.CountAsync(); } catch { }
            if (rowCount == 0) return;

            for (int i = 0; i < rowCount; i++)
            {
                var row = rows.Nth(i);

                // teams key
                string keyTeams = "";
                try
                {
                    var names = await row.Locator("p.font-weight-bold").AllInnerTextsAsync();
                    var list = names.Select(t => Regex.Replace((t ?? "").Replace('\u00A0', ' ').Trim(), @"\s+", " "))
                                    .Where(t => !string.Equals(t, "vs", StringComparison.OrdinalIgnoreCase))
                                    .Take(2).ToList();
                    if (list.Count == 2) keyTeams = $"{list[0]} vs {list[1]}";
                }
                catch { }

                if (string.IsNullOrWhiteSpace(keyTeams) || !byTeams.TryGetValue(keyTeams, out var match)) continue;

                var rowContainer = row.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]").First;
                try { await rowContainer.ScrollIntoViewIfNeededAsync(); } catch { }
                await page.WaitForTimeoutAsync(80);

                // Try dropdown first (collect all lines near this button)
                var dropdownBtn = rowContainer.Locator(".gridInterernaQuotazioni button.dropdown-toggle:not(.dropdown-toggle-no-arrow)")
                                              .Filter(new() { Has = rowContainer.Locator("span.uo-selezionato") })
                                              .First;
                bool got = false;
                try
                {
                    if (await dropdownBtn.IsVisibleAsync(new() { Timeout = 500 }))
                    {
                        var opened = await OpenDropdownNearAsync(page, dropdownBtn);
                        if (opened)
                        {
                            var tuples = await ParseNearestOUTotalMenuAsync(page, dropdownBtn); // (raw, UNDER, OVER)
                            if (tuples != null && tuples.Count > 0)
                            {
                                foreach (var (raw, under, over) in tuples)
                                {
                                    var line = NormalizeOUTotalLine(raw);
                                    if (string.IsNullOrWhiteSpace(line)) continue;

                                    var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                    if (!string.IsNullOrWhiteSpace(under)) sides["UNDER"] = under.Trim();
                                    if (!string.IsNullOrWhiteSpace(over)) sides["OVER"] = over.Trim();

                                    if (sides.Count > 0) match.OUTotals[line] = sides;
                                }
                                got = true;
                            }
                        }
                    }
                }
                catch { }
                finally
                {
                    try { await dropdownBtn.ClickAsync(new() { Timeout = 300, Force = true }); } catch { }
                }

                // Inline fallback (no dropdown menu)
                if (!got)
                {
                    try
                    {
                        var inline = await ExtractInlineOUTotalsAsync(rowContainer);
                        if (inline is { } gotInline)
                        {
                            var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                            if (!string.IsNullOrWhiteSpace(gotInline.under)) sides["UNDER"] = gotInline.under;
                            if (!string.IsNullOrWhiteSpace(gotInline.over)) sides["OVER"] = gotInline.over;

                            if (sides.Count > 0)
                                match.OUTotals[NormalizeOUTotalLine(gotInline.line)] = sides;
                        }
                    }
                    catch { }
                }
            }

            Console.WriteLine("[BB/O-U] Enrichment done.");
        }

        private static async Task EnrichIceHockeyOUAsync(IPage page, List<MatchData> matches)
        {
            if (matches == null || matches.Count == 0)
            {
                Console.WriteLine("[HK] No matches for O/U enrichment — skipping.");
                return;
            }

            // Open the global Under/Over filter once (your existing helper)
            try
            {
                await OpenOUFilterFromFirstFixtureAsync(page);
                var opened = await ClickVisibleOUFilterAsync(page);
                if (!opened)
                {
                    Console.WriteLine("[HK] Under/Over macro filter not found — skipping O/U.");
                    return;
                }
            }
            catch
            {
                Console.WriteLine("[HK] Error opening the O/U macro — skipping O/U.");
                return;
            }

            await HandleRandomPopup(page);
            await page.WaitForTimeoutAsync(150);

            // Index matches by team key
            var byTeams = new Dictionary<string, MatchData>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in matches)
            {
                var k = (m?.Teams ?? "").Replace('\u00A0', ' ').Trim();
                if (!string.IsNullOrWhiteSpace(k) && !byTeams.ContainsKey(k)) byTeams[k] = m;
                m.OUTotals ??= new(StringComparer.OrdinalIgnoreCase);
            }

            var rows = page.Locator(".tabellaQuoteSquadre");
            int rowCount = 0; try { rowCount = await rows.CountAsync(); } catch { }
            if (rowCount == 0) return;

            for (int i = 0; i < rowCount; i++)
            {
                var row = rows.Nth(i);

                // Resolve team key
                string keyTeams = "";
                try
                {
                    var names = await row.Locator("p.font-weight-bold").AllInnerTextsAsync();
                    var list = names.Select(t => Regex.Replace((t ?? "").Replace('\u00A0', ' ').Trim(), @"\s+", " "))
                                    .Where(t => !string.Equals(t, "vs", StringComparison.OrdinalIgnoreCase))
                                    .Take(2).ToList();
                    if (list.Count == 2) keyTeams = $"{list[0]} vs {list[1]}";
                }
                catch { }

                if (string.IsNullOrWhiteSpace(keyTeams) || !byTeams.TryGetValue(keyTeams, out var match)) continue;

                var rowContainer = row.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]").First;
                try { await rowContainer.ScrollIntoViewIfNeededAsync(); } catch { }
                await page.WaitForTimeoutAsync(80);

                // ---- 1) DROPDOWN FIRST (exclude "no-arrow") ----
                var dropdownBtn = rowContainer.Locator(".gridInterernaQuotazioni button.dropdown-toggle:not(.dropdown-toggle-no-arrow)")
                                              .Filter(new() { Has = rowContainer.Locator("span.uo-selezionato") })
                                              .First;

                bool gotFromDropdown = false;
                try
                {
                    if (await dropdownBtn.IsVisibleAsync(new() { Timeout = 500 }))
                    {
                        // Try to open a menu near THIS button
                        var opened = await OpenDropdownNearAsync(page, dropdownBtn);
                        if (opened)
                        {
                            var tuples = await ParseNearestOUTotalMenuAsync(page, dropdownBtn); // (rawLine, UNDER, OVER)
                            if (tuples != null && tuples.Count > 0)
                            {
                                foreach (var (raw, under, over) in tuples)
                                {
                                    var line = NormalizeOUTotalLine(raw);
                                    if (string.IsNullOrWhiteSpace(line)) continue;

                                    var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                    if (!string.IsNullOrWhiteSpace(under)) sides["UNDER"] = under.Trim();
                                    if (!string.IsNullOrWhiteSpace(over)) sides["OVER"] = over.Trim();

                                    if (sides.Count > 0) match.OUTotals[line] = sides;
                                }
                                gotFromDropdown = true;
                            }
                        }
                    }
                }
                catch { }
                finally
                {
                    try { await dropdownBtn.ClickAsync(new() { Timeout = 300, Force = true }); } catch { /* close if open */ }
                }

                // ---- 2) INLINE FALLBACK (no dropdown or dropdown absent) ----
                if (!gotFromDropdown)
                {
                    try
                    {
                        var inline = await ExtractInlineOUTotalsAsync(rowContainer);
                        if (inline is { } gotInline)
                        {
                            var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                            if (!string.IsNullOrWhiteSpace(gotInline.under)) sides["UNDER"] = gotInline.under;
                            if (!string.IsNullOrWhiteSpace(gotInline.over)) sides["OVER"] = gotInline.over;

                            if (sides.Count > 0)
                                match.OUTotals[NormalizeOUTotalLine(gotInline.line)] = sides;
                        }
                    }
                    catch { }
                }
            }

            Console.WriteLine("[HK] O/U enrichment done (dropdown-first, inline fallback).");
        }



        // Find the O/U dropdown toggle inside a fixture row
        private static async Task<ILocator?> FindRowOUToggleAsync(ILocator rowContainer)
        {
            // Prefer a toggle that shows the selected O/U line and is a real dropdown (has arrow)
            var btn = rowContainer.Locator("button.dropdown-toggle:not(.dropdown-toggle-no-arrow):has(span.uo-selezionato)");
            if (await btn.IsVisibleAsync(new() { Timeout = 400 })) return btn;

            // Generic dropdown toggles in the odds grid (exclude no-arrow)
            btn = rowContainer.Locator(".gridInterernaQuotazioni button.dropdown-toggle:not(.dropdown-toggle-no-arrow)");
            if (await btn.IsVisibleAsync(new() { Timeout = 400 })) return btn;

            // Last resort (still exclude no-arrow)
            btn = rowContainer.Locator("button.dropdown-toggle:not(.dropdown-toggle-no-arrow)");
            if (await btn.IsVisibleAsync(new() { Timeout = 400 })) return btn;

            return null; // no dropdown present; inline only
        }

        // Open the found toggle (with fallbacks) and ensure a dropdown-menu is shown near it
        private static async Task<bool> OpenDropdownNearAsync(IPage page, ILocator toggle)
        {

            try
            {
                var cls = await toggle.GetAttributeAsync("class");
                if (!string.IsNullOrEmpty(cls) && cls.Contains("dropdown-toggle-no-arrow", StringComparison.OrdinalIgnoreCase))
                    return false;
            }
            catch { }
            // Bring it into view and try a normal click first
            try { await toggle.ScrollIntoViewIfNeededAsync(); } catch { }
            await page.WaitForTimeoutAsync(60);

            // 1) standard click
            try
            {
                await toggle.ClickAsync(new() { Timeout = 1200 });
                try { await page.WaitForSelectorAsync(".dropdown-menu.show", new() { Timeout = 800, State = WaitForSelectorState.Visible }); } catch { }
                // if any menu is visible, we’re good — the nearest one will be parsed by your existing parser
                if (await page.Locator(".dropdown-menu.show").IsVisibleAsync(new() { Timeout = 200 })) return true;
            }
            catch { /* fall through */ }

            // 2) JS dispatch (beats overlay/strict-mode issues)
            try
            {
                var h = await toggle.ElementHandleAsync();
                if (h != null)
                {
                    await page.EvaluateAsync("el => el && el.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,view:window}))", h);
                    try { await page.WaitForSelectorAsync(".dropdown-menu.show", new() { Timeout = 800, State = WaitForSelectorState.Visible }); } catch { }
                    if (await page.Locator(".dropdown-menu.show").IsVisibleAsync(new() { Timeout = 200 })) return true;
                }
            }
            catch { /* fall through */ }

            // 3) Coordinate click fallback
            try
            {
                var h = await toggle.ElementHandleAsync();
                var box = await h?.BoundingBoxAsync();
                if (box != null)
                {
                    await page.Mouse.MoveAsync(box.X + box.Width / 2, box.Y + box.Height / 2);
                    await page.Mouse.DownAsync();
                    await page.Mouse.UpAsync();
                    try { await page.WaitForSelectorAsync(".dropdown-menu.show", new() { Timeout = 800, State = WaitForSelectorState.Visible }); } catch { }
                    if (await page.Locator(".dropdown-menu.show").IsVisibleAsync(new() { Timeout = 200 })) return true;
                }
            }
            catch { }

            return false;
        }



        private static async Task<bool> ClickHK_OUFilter_BottomStartAsync(IPage page)
        {
            await HandleRandomPopup(page);

            // ---- Helpers (scoped) ----
            async Task<bool> VerifyOpenedAsync()
            {
                try
                {
                    var anyOU = page.Locator(
                        ".tabellaQuoteSquadre button.dropdown-toggle:has(span.uo-selezionato), " +
                        ".tabellaQuoteSquadre .gridInterernaQuotazioni button.dropdown-toggle");
                    return await anyOU.IsVisibleAsync(new() { Timeout = 700 });
                }
                catch { return false; }
            }

            async Task<IReadOnlyList<IElementHandle>> GetRowsAsync()
            {
                var loc = page.Locator(".tabellaQuoteSquadre");
                int count = 0; try { count = await loc.CountAsync(); } catch { }
                var list = new List<IElementHandle>(Math.Max(0, count));
                for (int i = 0; i < count; i++)
                {
                    var h = await loc.Nth(i).ElementHandleAsync();
                    if (h != null) list.Add(h);
                }
                return list;
            }

            async Task ScrollFixtureIntoViewAsync(IElementHandle row)
            {
                try
                {
                    await page.EvaluateAsync(@"(el) => {
                try { el.scrollIntoView({ block: 'center', inline: 'nearest', behavior: 'instant' }); } catch {}
            }", row);
                }
                catch { }
                try { await page.Mouse.WheelAsync(0, 40); } catch { }
                await page.WaitForTimeoutAsync(70);
            }

            async Task EnsureAtTopAsync()
            {
                for (int i = 0; i < 30; i++)
                {
                    int top = 0;
                    try
                    {
                        top = await page.EvaluateAsync<int>(
                            "() => (document.scrollingElement||document.documentElement||document.body).scrollTop|0");
                    }
                    catch { }
                    if (top <= 2) return;
                    try { await page.Mouse.WheelAsync(0, -400); } catch { }
                    await page.WaitForTimeoutAsync(60);
                }
            }

            async Task<ILocator?> PickClosestVisibleTileAsync()
            {
                // Prefer data-id match; fallback to text-only
                var candidates = page.Locator("div.filter.Zprincipali[data-id='a-606']")
                                     .Filter(new() { HasTextString = "Under/Over" });
                if (await candidates.CountAsync() == 0)
                    candidates = page.Locator("div.filter.Zprincipali")
                                     .Filter(new() { HasTextString = "Under/Over" });

                int count = 0; try { count = await candidates.CountAsync(); } catch { }
                if (count == 0) return null;

                int bestIdx = -1;
                double bestTop = double.PositiveInfinity;

                for (int i = 0; i < count; i++)
                {
                    var cand = candidates.Nth(i);
                    if (!await cand.IsVisibleAsync(new() { Timeout = 250 })) continue;

                    var handle = await cand.ElementHandleAsync();
                    if (handle == null) continue;

                    var box = await handle.BoundingBoxAsync();
                    if (box == null) continue;

                    if (box.Y >= 0 && box.Y < bestTop)
                    {
                        bestTop = box.Y;
                        bestIdx = i;
                    }
                }

                if (bestIdx >= 0) return candidates.Nth(bestIdx);

                // Last resort: first visible
                for (int i = 0; i < count; i++)
                {
                    var cand = candidates.Nth(i);
                    if (await cand.IsVisibleAsync(new() { Timeout = 250 })) return cand;
                }
                return null;
            }

            // ---- Start from bottom (after 1/2 extraction), walk UP fixture-by-fixture ----
            var rows = await GetRowsAsync();
            if (rows.Count == 0) return false;

            // scroller diagnostics
            IJSHandle? sc = null;
            try { sc = await page.EvaluateHandleAsync("() => (document.scrollingElement||document.documentElement||document.body)"); } catch { }
            var scTop = sc != null ? await page.EvaluateAsync<int>("el => el.scrollTop|0", sc) : -1;
            var scMax = sc != null ? await page.EvaluateAsync<int>("el => (el.scrollHeight - el.clientHeight)|0", sc) : -1;
            Console.WriteLine($"[HK] scroller top={scTop} max={scMax}, rows={rows.Count}");

            for (int i = rows.Count - 1; i >= 0; i--)
                await ScrollFixtureIntoViewAsync(rows[i]);

            // make sure we truly reach top (without snap)
            await EnsureAtTopAsync();
            try { await page.EvaluateAsync("() => document.body?.click()"); } catch { }
            await page.WaitForTimeoutAsync(120);

            // re-snapshot (virtualization at top)
            rows = await GetRowsAsync();
            if (rows.Count == 0) return false;

            var tileByDataIdOrText = page.Locator("div.filter.Zprincipali[data-id='a-606']").Filter(new() { HasTextString = "Under/Over" });
            var tileByTextOnly = page.Locator("div.filter.Zprincipali").Filter(new() { HasTextString = "Under/Over" });

            // Walk DOWN until any O/U tile is visible
            bool seen = await tileByDataIdOrText.IsVisibleAsync(new() { Timeout = 150 }) ||
                        await tileByTextOnly.IsVisibleAsync(new() { Timeout = 150 });

            for (int i = 0; i < rows.Count && !seen; i++)
            {
                await ScrollFixtureIntoViewAsync(rows[i]);
                try { await page.Mouse.WheelAsync(0, 60); } catch { }
                await page.WaitForTimeoutAsync(70);

                seen = await tileByDataIdOrText.IsVisibleAsync(new() { Timeout = 150 }) ||
                       await tileByTextOnly.IsVisibleAsync(new() { Timeout = 150 });
            }

            if (!seen)
            {
                // extra nudges
                for (int k = 0; k < 6 && !seen; k++)
                {
                    try { await page.Mouse.WheelAsync(0, 140); } catch { }
                    await page.WaitForTimeoutAsync(80);
                    seen = await tileByDataIdOrText.IsVisibleAsync(new() { Timeout = 150 }) ||
                           await tileByTextOnly.IsVisibleAsync(new() { Timeout = 150 });
                }
            }

            if (!seen) return false;

            // ---- Click exactly ONE tile (dedupe strict-mode) ----
            try
            {
                var target = await PickClosestVisibleTileAsync();
                if (target != null)
                {
                    await target.ScrollIntoViewIfNeededAsync();
                    await target.ClickAsync(new() { Timeout = 1500 });
                    await page.WaitForTimeoutAsync(150);
                    if (await VerifyOpenedAsync()) return true;
                }
            }
            catch { /* continue to fallbacks */ }

            // JS-click fallback on chosen element
            try
            {
                var chosen = await page.EvaluateHandleAsync(@"() => {
            const vis = el => {
              if (!el) return false;
              const s = getComputedStyle(el), r = el.getBoundingClientRect();
              return s.visibility !== 'hidden' && s.display !== 'none' && r.width > 0 && r.height > 0;
            };
            const tiles = [...document.querySelectorAll('div.filter.Zprincipali[data-id=""a-606""]')]
              .filter(t => (t.textContent||'').includes('Under/Over') && vis(t));
            if (!tiles.length) return null;
            tiles.sort((a,b) => a.getBoundingClientRect().top - b.getBoundingClientRect().top);
            return tiles[0];
        }");
                if (chosen != null)
                {
                    await page.EvaluateAsync(@"el => el && el.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,view:window}))", chosen);
                    await page.WaitForTimeoutAsync(150);
                    if (await VerifyOpenedAsync()) return true;
                }
            }
            catch { }

            // Coordinate click fallback
            try
            {
                var candidates = page.Locator("div.filter.Zprincipali[data-id='a-606']")
                                     .Filter(new() { HasTextString = "Under/Over" });
                int c = 0; try { c = await candidates.CountAsync(); } catch { }
                for (int i = 0; i < c; i++)
                {
                    var cand = candidates.Nth(i);
                    if (!await cand.IsVisibleAsync(new() { Timeout = 250 })) continue;
                    var h = await cand.ElementHandleAsync();
                    var box = await h?.BoundingBoxAsync();
                    if (box == null) continue;

                    await page.Mouse.MoveAsync(box.X + box.Width / 2, box.Y + box.Height / 2);
                    await page.Mouse.DownAsync();
                    await page.Mouse.UpAsync();
                    await page.WaitForTimeoutAsync(150);
                    if (await VerifyOpenedAsync()) return true;
                    break; // clicked one; stop trying others
                }
            }
            catch { }

            return false;
        }

        private static async Task EnrichIceHockeyHandicapAsync(IPage page, List<MatchData> matches)
        {
            if (matches == null || matches.Count == 0) return;


            // Index matches by team string
            var byTeams = new Dictionary<string, MatchData>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in matches)
            {
                var k = (m?.Teams ?? "").Replace('\u00A0', ' ').Trim();
                if (!string.IsNullOrWhiteSpace(k) && !byTeams.ContainsKey(k)) byTeams[k] = m;
                m.TTPlusHandicap ??= new(StringComparer.OrdinalIgnoreCase);
            }

            var rows2 = page.Locator(".tabellaQuoteSquadre");
            int rowCount = 0; try { rowCount = await rows2.CountAsync(); } catch { }
            int rowsUpdated = 0;

            for (int i = 0; i < rowCount; i++)
            {
                var row = rows2.Nth(i);

                // resolve team key
                string keyTeams = "";
                try
                {
                    var names = await row.Locator("p.font-weight-bold").AllInnerTextsAsync();
                    var list = names.Select(t => Regex.Replace((t ?? "").Replace('\u00A0', ' ').Trim(), @"\s+", " "))
                                    .Where(t => !string.Equals(t, "vs", StringComparison.OrdinalIgnoreCase))
                                    .Take(2).ToList();
                    if (list.Count == 2) keyTeams = $"{list[0]} vs {list[1]}";
                }
                catch { }

                if (string.IsNullOrWhiteSpace(keyTeams) || !byTeams.TryGetValue(keyTeams, out var match)) continue;

                var rowContainer = row.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]").First;
                try { await rowContainer.ScrollIntoViewIfNeededAsync(); } catch { }
                await page.WaitForTimeoutAsync(80);

                // Inline selected handicap without arrow
                try
                {
                    var inline = await ExtractInlineHcpNoDropdownAsync(rowContainer);
                    if (inline is { } found)
                    {
                        var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        if (!string.IsNullOrWhiteSpace(found.one)) sides["1"] = found.one;
                        if (!string.IsNullOrWhiteSpace(found.two)) sides["2"] = found.two;
                        if (sides.Count > 0) match.TTPlusHandicap[found.line] = sides;
                    }
                }
                catch { }

                // Dropdown handicap → parse all lines (1/2)
                var buttons = rowContainer.Locator("button.dropdown-toggle");
                int btnCount = 0; try { btnCount = await buttons.CountAsync(); } catch { }
                if (btnCount == 0) continue;

                for (int b = 0; b < btnCount; b++)
                {
                    var btn = buttons.Nth(b);
                    try
                    {
                        if (!await btn.IsVisibleAsync(new() { Timeout = 600 })) continue;

                        await btn.ClickAsync(new() { Timeout = 1500 });
                        try
                        {
                            await page.WaitForSelectorAsync(".dropdown-menu.show", new() { State = WaitForSelectorState.Visible, Timeout = 1200 });
                            await page.WaitForTimeoutAsync(120);
                        }
                        catch { }

                        var tuples = await ParseNearestTTMenuAsync(page, btn); // (rawLine, 1, 2)
                        if (tuples != null && tuples.Count > 0)
                        {
                            foreach (var (raw, one, two) in tuples)
                            {
                                var line = NormalizeHcpLine(raw);
                                if (string.IsNullOrWhiteSpace(line)) continue;

                                var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                if (!string.IsNullOrWhiteSpace(one)) sides["1"] = one.Trim();
                                if (!string.IsNullOrWhiteSpace(two)) sides["2"] = two.Trim();
                                if (sides.Count > 0) match.TTPlusHandicap[line] = sides;
                            }
                            rowsUpdated++;
                            break; // one dropdown per row is enough
                        }
                    }
                    catch { }
                    finally
                    {
                        try { await btn.ClickAsync(new() { Timeout = 500, Force = true }); } catch { }
                    }
                }
            }

            Console.WriteLine($"[HK/Handicap] Enrichment done. Rows updated: {rowsUpdated}.");
        }


        // AMERICAN FOOTBALL: collect O/U (inline selected + dropdown menu if present)
        private static async Task EnrichAmericanFootballOUAsync(IPage page, List<MatchData> matches)
        {
            if (matches == null || matches.Count == 0) return;

            // Try clicking an Under/Over filter if present, but proceed even if not found
            try
            {
                var tile = page.Locator("div.filter.Zprincipali >> text=Under/Over");
                if (await tile.IsVisibleAsync(new() { Timeout = 700 })) { await tile.ClickAsync(); }
                else
                {
                    var alt = page.GetByText("Under/Over")
                                  .Locator("xpath=ancestor::div[contains(@class,'filter') and contains(@class,'Zprincipali')]");
                    if (await alt.IsVisibleAsync(new() { Timeout = 700 })) await alt.ClickAsync();
                }
            }
            catch { }

            var byTeams = new Dictionary<string, MatchData>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in matches)
            {
                var k = (m?.Teams ?? "").Replace('\u00A0', ' ').Trim();
                if (!string.IsNullOrWhiteSpace(k) && !byTeams.ContainsKey(k)) byTeams[k] = m;
                m.OUTotals ??= new(StringComparer.OrdinalIgnoreCase);
            }

            var rows = page.Locator(".tabellaQuoteSquadre");
            int rowCount = 0; try { rowCount = await rows.CountAsync(); } catch { }
            if (rowCount == 0) return;

            for (int i = 0; i < rowCount; i++)
            {
                var row = rows.Nth(i);

                // team key
                string keyTeams = "";
                try
                {
                    var names = await row.Locator("p.font-weight-bold").AllInnerTextsAsync();
                    var list = names.Select(t => Regex.Replace((t ?? "").Replace('\u00A0', ' ').Trim(), @"\s+", " "))
                                    .Where(t => !string.Equals(t, "vs", StringComparison.OrdinalIgnoreCase))
                                    .Take(2).ToList();
                    if (list.Count == 2) keyTeams = $"{list[0]} vs {list[1]}";
                }
                catch { }
                if (string.IsNullOrWhiteSpace(keyTeams) || !byTeams.TryGetValue(keyTeams, out var match)) continue;

                var rowContainer = row.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]").First;
                await KeepInViewAsync(page, rowContainer);

                // A) Inline selected O/U
                try
                {
                    var inline = await ExtractInlineOUTotalsAsync(rowContainer);
                    if (inline is { } got)
                    {
                        var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        if (!string.IsNullOrWhiteSpace(got.under)) sides["UNDER"] = got.under;
                        if (!string.IsNullOrWhiteSpace(got.over)) sides["OVER"] = got.over;
                        if (sides.Count > 0) match.OUTotals[got.line] = sides;
                    }
                }
                catch { }

                // B) Dropdown O/U (if present) → collect all lines
                var ouBtn = rowContainer.Locator("button.dropdown-toggle:has(span.uo-selezionato)");
                if (!await ouBtn.IsVisibleAsync(new() { Timeout = 500 }))
                    ouBtn = rowContainer.Locator("button.dropdown-toggle").First;

                try
                {
                    if (await ouBtn.IsVisibleAsync(new() { Timeout = 500 }))
                    {
                        await KeepInViewAsync(page, ouBtn);
                        await ouBtn.ClickAsync(new() { Timeout = 1500 });
                        try
                        {
                            await page.WaitForSelectorAsync(".dropdown-menu.show", new() { State = WaitForSelectorState.Visible, Timeout = 1200 });
                            await page.WaitForTimeoutAsync(120);
                        }
                        catch { }

                        var tuples = await ParseNearestOUTotalMenuAsync(page, ouBtn); // (rawLine, UNDER, OVER)
                        if (tuples != null && tuples.Count > 0)
                        {
                            foreach (var (raw, under, over) in tuples)
                            {
                                var line = NormalizeOUTotalLine(raw);
                                if (string.IsNullOrWhiteSpace(line)) continue;

                                var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                if (!string.IsNullOrWhiteSpace(under)) sides["UNDER"] = under.Trim();
                                if (!string.IsNullOrWhiteSpace(over)) sides["OVER"] = over.Trim();
                                if (sides.Count > 0) match.OUTotals[line] = sides;
                            }
                        }
                    }
                }
                catch { }
                finally
                {
                    try { await ouBtn.ClickAsync(new() { Timeout = 500, Force = true }); } catch { }
                }
            }
        }

        // =========================
        // AMERICAN FOOTBALL: Handicap filter + dropdowns → parse 1/2 for all lines (ONLY AF touched)
        // =========================
        private static async Task EnrichAmericanFootballHandicapAsync(IPage page, List<MatchData> matches)
        {
            if (matches == null || matches.Count == 0) return;

            // 1) Click the "Handicap" filter (data-id='a-1860'), fallback to text
            bool clickedFilter = false;
            try
            {
                var tile = page.Locator("div.filter.Zprincipali[data-id='a-1860'] >> text=Handicap");
                if (await tile.IsVisibleAsync(new() { Timeout = 800 }))
                {
                    await tile.ClickAsync();
                    clickedFilter = true;
                }
            }
            catch { }

            if (!clickedFilter)
            {
                try
                {
                    var alt = page.GetByText("Handicap")
                                  .Locator("xpath=ancestor::div[contains(@class,'filter') and contains(@class,'Zprincipali')]");
                    if (await alt.IsVisibleAsync(new() { Timeout = 800 }))
                    {
                        await alt.ClickAsync();
                        clickedFilter = true;
                    }
                }
                catch { }
            }

            if (!clickedFilter)
            {
                Console.WriteLine("[AF/Handicap] Handicap filter not found — skipping.");
                return;
            }

            await page.WaitForTimeoutAsync(250);

            // 2) Index by Teams
            var byTeams = new Dictionary<string, MatchData>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in matches)
            {
                var k = (m?.Teams ?? "").Replace('\u00A0', ' ').Trim();
                if (!string.IsNullOrWhiteSpace(k) && !byTeams.ContainsKey(k))
                    byTeams[k] = m;
                m.TTPlusHandicap ??= new(StringComparer.OrdinalIgnoreCase);
            }

            // 3) Iterate rows → open handicap dropdown → parse
            var rows = page.Locator(".tabellaQuoteSquadre");
            int rowCount = 0; try { rowCount = await rows.CountAsync(); } catch { }
            if (rowCount == 0) return;

            int rowsUpdated = 0;

            for (int i = 0; i < rowCount; i++)
            {
                var row = rows.Nth(i);

                // Row → team key
                string keyTeams = "";
                try
                {
                    var names = await row.Locator("p.font-weight-bold").AllInnerTextsAsync();
                    var list = names.Select(t => Regex.Replace((t ?? "").Replace('\u00A0', ' ').Trim(), @"\s+", " "))
                                    .Where(t => !string.Equals(t, "vs", StringComparison.OrdinalIgnoreCase))
                                    .Take(2)
                                    .ToList();
                    if (list.Count == 2) keyTeams = $"{list[0]} vs {list[1]}";
                }
                catch { }

                if (string.IsNullOrWhiteSpace(keyTeams) || !byTeams.TryGetValue(keyTeams, out var match))
                    continue;

                var rowContainer = row.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]").First;

                // Keep row in view (AF only)
                await KeepInViewAsync(page, rowContainer);

                // NEW: capture inline handicap when there's no dropdown arrow
                try
                {
                    var inline = await ExtractInlineHcpNoDropdownAsync(rowContainer);
                    if (inline is { } found)
                    {
                        var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        if (!string.IsNullOrWhiteSpace(found.one)) sides["1"] = found.one;
                        if (!string.IsNullOrWhiteSpace(found.two)) sides["2"] = found.two;

                        if (sides.Count > 0)
                            match.TTPlusHandicap[found.line] = sides;
                    }
                }
                catch { /* non-fatal */ }


                var buttons = rowContainer.Locator("button.dropdown-toggle");
                int btnCount = 0; try { btnCount = await buttons.CountAsync(); } catch { }
                if (btnCount == 0) continue;

                for (int b = 0; b < btnCount; b++)
                {
                    var btn = buttons.Nth(b);
                    try
                    {
                        if (!await btn.IsVisibleAsync(new() { Timeout = 600 })) continue;

                        await KeepInViewAsync(page, btn);
                        await btn.ClickAsync(new() { Timeout = 1500 });

                        try
                        {
                            await page.WaitForSelectorAsync(".dropdown-menu.show", new() { State = WaitForSelectorState.Visible, Timeout = 1200 });
                            await page.WaitForTimeoutAsync(120);
                        }
                        catch { }

                        // Parse menu: (line, 1, 2)
                        var tuples = await ParseNearestTTMenuAsync(page, btn);
                        if (tuples != null && tuples.Count > 0)
                        {
                            foreach (var (raw, one, two) in tuples)
                            {
                                var line = NormalizeHcpLine(raw);
                                if (string.IsNullOrWhiteSpace(line)) continue;

                                var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                if (!string.IsNullOrWhiteSpace(one)) sides["1"] = one.Trim();
                                if (!string.IsNullOrWhiteSpace(two)) sides["2"] = two.Trim();

                                if (sides.Count > 0)
                                    match.TTPlusHandicap[line] = sides;
                            }

                            rowsUpdated++;
                            break; // one dropdown per row is enough
                        }
                    }
                    catch { }
                    finally
                    {
                        try { await btn.ClickAsync(new() { Timeout = 500, Force = true }); } catch { }
                    }
                }
            }

            Console.WriteLine($"[AF/Handicap] Enrichment done. Rows updated: {rowsUpdated}.");
        }

        // =========================
        // BASEBALL: Handicap filter + dropdowns → parse 1/2 for all lines
        // =========================
        private static async Task EnrichBaseballHandicapAsync(IPage page, List<MatchData> matches)
        {
            if (matches == null || matches.Count == 0) return;

            // 1) Click the "Handicap" filter for Baseball (data-id='a-432'), fallback to text
            bool clickedFilter = false;
            try
            {
                var tile = page.Locator("div.filter.Zprincipali[data-id='a-432'] >> text=Handicap");
                if (await tile.IsVisibleAsync(new() { Timeout = 800 }))
                {
                    await tile.ClickAsync();
                    clickedFilter = true;
                }
            }
            catch { }

            if (!clickedFilter)
            {
                try
                {
                    var alt = page.GetByText("Handicap")
                                  .Locator("xpath=ancestor::div[contains(@class,'filter') and contains(@class,'Zprincipali')]");
                    if (await alt.IsVisibleAsync(new() { Timeout = 800 }))
                    {
                        await alt.ClickAsync();
                        clickedFilter = true;
                    }
                }
                catch { }
            }

            if (!clickedFilter)
            {
                Console.WriteLine("[BB/Handicap] Handicap filter not found — skipping.");
                return;
            }

            await page.WaitForTimeoutAsync(250);

            // 2) Index by Teams
            var byTeams = new Dictionary<string, MatchData>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in matches)
            {
                var k = (m?.Teams ?? "").Replace('\u00A0', ' ').Trim();
                if (!string.IsNullOrWhiteSpace(k) && !byTeams.ContainsKey(k))
                    byTeams[k] = m;
                m.TTPlusHandicap ??= new(StringComparer.OrdinalIgnoreCase);
            }

            // 3) Iterate rows → capture inline + dropdown handicap
            var rows = page.Locator(".tabellaQuoteSquadre");
            int rowCount = 0; try { rowCount = await rows.CountAsync(); } catch { }
            if (rowCount == 0) return;

            int rowsUpdated = 0;

            for (int i = 0; i < rowCount; i++)
            {
                var row = rows.Nth(i);

                // Row → team key
                string keyTeams = "";
                try
                {
                    var names = await row.Locator("p.font-weight-bold").AllInnerTextsAsync();
                    var list = names.Select(t => Regex.Replace((t ?? "").Replace('\u00A0', ' ').Trim(), @"\s+", " "))
                                    .Where(t => !string.Equals(t, "vs", StringComparison.OrdinalIgnoreCase))
                                    .Take(2)
                                    .ToList();
                    if (list.Count == 2) keyTeams = $"{list[0]} vs {list[1]}";
                }
                catch { }

                if (string.IsNullOrWhiteSpace(keyTeams) || !byTeams.TryGetValue(keyTeams, out var match))
                    continue;

                var rowContainer = row.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]").First;

                // Keep in view for stability
                await KeepInViewAsync(page, rowContainer);

                // A) Inline selected handicap (no-dropdown arrow)
                try
                {
                    var inline = await ExtractInlineHcpNoDropdownAsync(rowContainer);
                    if (inline is { } found)
                    {
                        var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        if (!string.IsNullOrWhiteSpace(found.one)) sides["1"] = found.one;
                        if (!string.IsNullOrWhiteSpace(found.two)) sides["2"] = found.two;

                        if (sides.Count > 0)
                            match.TTPlusHandicap[found.line] = sides;
                    }
                }
                catch { /* non-fatal */ }

                // B) Dropdown handicap → parse all lines (1/2)
                var buttons = rowContainer.Locator("button.dropdown-toggle");
                int btnCount = 0; try { btnCount = await buttons.CountAsync(); } catch { }
                if (btnCount == 0) continue;

                for (int b = 0; b < btnCount; b++)
                {
                    var btn = buttons.Nth(b);
                    try
                    {
                        if (!await btn.IsVisibleAsync(new() { Timeout = 600 })) continue;

                        await KeepInViewAsync(page, btn);
                        await btn.ClickAsync(new() { Timeout = 1500 });

                        try
                        {
                            await page.WaitForSelectorAsync(".dropdown-menu.show", new() { State = WaitForSelectorState.Visible, Timeout = 1200 });
                            await page.WaitForTimeoutAsync(120);
                        }
                        catch { }

                        var tuples = await ParseNearestTTMenuAsync(page, btn); // (rawLine, 1, 2)
                        if (tuples != null && tuples.Count > 0)
                        {
                            foreach (var (raw, one, two) in tuples)
                            {
                                var line = NormalizeHcpLine(raw);
                                if (string.IsNullOrWhiteSpace(line)) continue;

                                var sides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                if (!string.IsNullOrWhiteSpace(one)) sides["1"] = one.Trim();
                                if (!string.IsNullOrWhiteSpace(two)) sides["2"] = two.Trim();

                                if (sides.Count > 0)
                                    match.TTPlusHandicap[line] = sides;
                            }

                            rowsUpdated++;
                            break; // one dropdown per row is enough
                        }
                    }
                    catch { }
                    finally
                    {
                        try { await btn.ClickAsync(new() { Timeout = 500, Force = true }); } catch { }
                    }
                }
            }

            Console.WriteLine($"[BB/Handicap] Enrichment done. Rows updated: {rowsUpdated}.");
        }

        // Extract inline (selected) O/U line and its UNDER/OVER odds from the visible row
        private static async Task<(string line, string under, string over)?> ExtractInlineOUTotalsAsync(ILocator rowContainer)
        {
            try
            {
                // Many skins show the selected O/U line here
                var lineSpan = rowContainer.Locator(".gridInterernaQuotazioni span.uo-selezionato");
                if (!await lineSpan.IsVisibleAsync(new() { Timeout = 400 })) return null;

                var rawLine = (await lineSpan.InnerTextAsync())?.Trim() ?? "";
                var line = NormalizeOUTotalLine(rawLine);
                if (string.IsNullOrWhiteSpace(line)) return null;

                // The visible UNDER / OVER cells in the same grid
                var cells = rowContainer.Locator(".gridInterernaQuotazioni .contenitoreSingolaQuota");
                int count = 0; try { count = await cells.CountAsync(); } catch { }
                if (count == 0) return null;

                string u = "", o = "";
                for (int i = 0; i < count; i++)
                {
                    try
                    {
                        var cell = cells.Nth(i);
                        var lab = ((await cell.Locator(".titoloQuotazione").InnerTextAsync()) ?? "").Trim().ToUpperInvariant();
                        var val = ((await cell.Locator(".tipoQuotazione_1").InnerTextAsync()) ?? "").Trim();
                        if (lab == "UNDER" && string.IsNullOrEmpty(u)) u = val;
                        else if (lab == "OVER" && string.IsNullOrEmpty(o)) o = val;
                    }
                    catch { }
                }

                if (string.IsNullOrWhiteSpace(u) && string.IsNullOrWhiteSpace(o)) return null;
                return (line, u, o);
            }
            catch { return null; }
        }


        // =========================
        // TT+ HANDICAP — HELPERS (UNCHANGED for other sports)
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

        // Call this right after you finish grabbing 1/2 odds for the league:
        // …then do your O/U per-row extraction (you can reuse your existing parse logic)

        private static async Task OpenOUFilterFromFirstFixtureAsync(IPage page)
        {
            await HandleRandomPopup(page);

            // 1) Get the list of fixture DOM nodes
            var rowLoc = page.Locator(".tabellaQuoteSquadre");
            int rowCount = 0; try { rowCount = await rowLoc.CountAsync(); } catch { }
            if (rowCount == 0) return;

            // 2) Find the actual scrollable container that holds fixtures
            var scroller = await GetMainScrollerAsync(page);

            // 3) From (likely) bottom → walk UP to the first fixture (center it), without going to page top
            for (int i = rowCount - 1; i >= 0; i--)
            {
                var handle = await rowLoc.Nth(i).ElementHandleAsync();
                if (handle == null) continue;
                await ScrollFixtureIntoViewAsync(page, handle, scroller);
            }

            // 4) Now the first fixture should be near the top. Ensure the *first fixture* is centered.
            var first = await rowLoc.Nth(0).ElementHandleAsync();
            if (first != null)
                await ScrollFixtureIntoViewAsync(page, first, scroller);

            // 5) If O/U filter visible here, click it; otherwise walk DOWN fixture-by-fixture until it appears
            if (!await ClickVisibleOUFilterAsync(page))
            {
                // Re-snapshot in case virtualization changed the DOM
                rowCount = 0; try { rowCount = await rowLoc.CountAsync(); } catch { }
                for (int i = 0; i < rowCount; i++)
                {
                    var handle = await rowLoc.Nth(i).ElementHandleAsync();
                    if (handle == null) continue;

                    await ScrollFixtureIntoViewAsync(page, handle, scroller);

                    if (await ClickVisibleOUFilterAsync(page))
                        break;
                }
            }
        }

        private static async Task<IElementHandle?> GetMainScrollerAsync(IPage page)
        {
            try
            {
                var h = await page.EvaluateHandleAsync(@"() => {
          const isScrollable = el => {
            if (!el) return false;
            const s = getComputedStyle(el);
            return (s.overflowY === 'auto' || s.overflowY === 'scroll') && el.scrollHeight > el.clientHeight;
          };
          // site usually wraps list in this block
          const pref = document.querySelector('#primo-blocco-sport');
          if (isScrollable(pref)) return pref;

          // nearest scrollable ancestor of a row
          const row = document.querySelector('.tabellaQuoteSquadre');
          let p = row ? row.parentElement : null;
          while (p) { if (isScrollable(p)) return p; p = p.parentElement; }

          // fallback: document scroller
          return document.scrollingElement || document.documentElement || document.body;
        }");
                return h.AsElement();
            }
            catch { return null; }
        }

        private static async Task ScrollFixtureIntoViewAsync(IPage page, IElementHandle row, IElementHandle? scroller)
        {
            try
            {
                // center the row (works even with sticky headers)
                await page.EvaluateAsync(@"el => { try{ el.scrollIntoView({block:'center', inline:'nearest', behavior:'instant'}); }catch{} }", row);
            }
            catch { }

            // small nudge to make observers update sticky toolbars
            try
            {
                if (scroller != null)
                    await page.EvaluateAsync("el => { try { el.scrollBy(0, 80); } catch {} }", scroller);
                else
                    await page.Mouse.WheelAsync(0, 80);
            }
            catch { }

            await page.WaitForTimeoutAsync(70);
        }

        private static ILocator OUFilterCandidates(IPage page)
        {
            // Prefer the data-id (a-606). Fallback to text "Under/Over".
            // Combined as a single selector with a comma.
            return page.Locator("div.filter.Zprincipali[data-id='a-606'], div.filter.Zprincipali:has-text(\"Under/Over\")");
        }

        private static async Task<bool> ClickVisibleOUFilterAsync(IPage page)
        {
            await HandleRandomPopup(page);

            var candidates = OUFilterCandidates(page);

            int count = 0; try { count = await candidates.CountAsync(); } catch { }
            if (count == 0) return false;

            // Pick the top-most visible candidate to avoid strict-mode collisions
            int bestIdx = -1;
            double bestTop = double.PositiveInfinity;

            for (int i = 0; i < count; i++)
            {
                var cand = candidates.Nth(i);
                if (!await cand.IsVisibleAsync(new() { Timeout = 250 })) continue;

                var h = await cand.ElementHandleAsync();
                if (h == null) continue;

                var box = await h.BoundingBoxAsync();
                if (box == null) continue;

                if (box.Y >= 0 && box.Y < bestTop)
                {
                    bestTop = box.Y;
                    bestIdx = i;
                }
            }

            if (bestIdx < 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (await candidates.Nth(i).IsVisibleAsync(new() { Timeout = 250 }))
                    {
                        bestIdx = i;
                        break;
                    }
                }
                if (bestIdx < 0) return false;
            }

            var target = candidates.Nth(bestIdx);

            try
            {
                await target.ScrollIntoViewIfNeededAsync();
                await target.ClickAsync(new() { Timeout = 1500 });

                // Verify an O/U dropdown is now available somewhere in fixtures
                var anyOU = page.Locator(
                    ".tabellaQuoteSquadre button.dropdown-toggle:has(span.uo-selezionato), " +
                    ".tabellaQuoteSquadre .gridInterernaQuotazioni button.dropdown-toggle");
                return await anyOU.IsVisibleAsync(new() { Timeout = 800 });
            }
            catch
            {
                // JS-dispatch fallback for weird overlays
                try
                {
                    var h = await target.ElementHandleAsync();
                    if (h != null)
                    {
                        await page.EvaluateAsync("el => el && el.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,view:window}))", h);
                        var anyOU = page.Locator(
                            ".tabellaQuoteSquadre button.dropdown-toggle:has(span.uo-selezionato), " +
                            ".tabellaQuoteSquadre .gridInterernaQuotazioni button.dropdown-toggle");
                        return await anyOU.IsVisibleAsync(new() { Timeout = 800 });
                    }
                }
                catch { }
                return false;
            }
        }


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
        // Utils (UNCHANGED except Baseball additions)
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

        private static readonly HashSet<string> RugbySyn = new(StringComparer.OrdinalIgnoreCase)
        { "RUGBY", "RUGBY UNION", "RUGBY LEAGUE" };

        private static readonly HashSet<string> AmericanFootballSyn = new(StringComparer.OrdinalIgnoreCase)
        { "FOOTBALL AMERICANO", "AMERICAN FOOTBALL", "AMERICAN-FOOTBALL", "NFL" };

        private static readonly HashSet<string> BaseballSyn = new(StringComparer.OrdinalIgnoreCase)
        { "BASEBALL" };
        // Ice Hockey synonyms & href patterns
        private static readonly HashSet<string> IceHockeySyn = new(StringComparer.OrdinalIgnoreCase)
{ "HOCKEY SU GHIACCIO", "ICE HOCKEY", "ICE-HOCKEY", "HOCKEY" };

        private static readonly string[] IceHockeyHrefKeys = { "/hockey-su-ghiaccio", "/ice-hockey", "/hockey" };

        private static readonly string[] AmerFootballHrefKeys = { "/football-americano", "/american-football" };
        private static readonly string[] CalcioHrefKeys = { "/calcio", "/football", "/soccer" };
        private static readonly string[] BasketHrefKeys = { "/pallacanestro", "/basketball" };
        private static readonly string[] TennisHrefKeys = { "/tennis" };
        private static readonly string[] RugbyHrefKeys = { "/rugby" };
        private static readonly string[] BaseballHrefKeys = { "/baseball" };

        private static string? CanonicalFromTextOrHref(string text, string? href)
        {
            var c = Canon(text);

            // text match
            // text match
            if (CalcioSyn.Contains(c)) return "Calcio";
            if (BasketSyn.Contains(c)) return "Pallacanestro";
            if (TennisSyn.Contains(c)) return "Tennis";
            if (RugbySyn.Contains(c)) return "Rugby";
            if (AmericanFootballSyn.Contains(c)) return "Football Americano";
            if (BaseballSyn.Contains(c)) return "Baseball";
            if (IceHockeySyn.Contains(c)) return "Hockey su ghiaccio";             // <-- ADD

            // href match
            href = (href ?? "").ToLowerInvariant();
            if (CalcioHrefKeys.Any(k => href.Contains(k))) return "Calcio";
            if (BasketHrefKeys.Any(k => href.Contains(k))) return "Pallacanestro";
            if (TennisHrefKeys.Any(k => href.Contains(k))) return "Tennis";
            if (RugbyHrefKeys.Any(k => href.Contains(k))) return "Rugby";
            if (AmerFootballHrefKeys.Any(k => href.Contains(k))) return "Football Americano";
            if (BaseballHrefKeys.Any(k => href.Contains(k))) return "Baseball";
            if (IceHockeyHrefKeys.Any(k => href.Contains(k))) return "Hockey su ghiaccio";  // <-- ADD


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
            if (L is "AMERICAN FOOTBALL" or "AMERICAN-FOOTBALL" or "FOOTBALL AMERICANO" or "NFL")
                return "Football Americano";
            if (L is "BASEBALL") return "Baseball";
            if (L is "BASEBALL") return "Baseball";
            if (L is "ICE-HOCKEY" or "ICE HOCKEY" or "HOCKEY SU GHIACCIO" or "HOCKEY")   // <-- ADD
                return "Hockey su ghiaccio";

            return s.Trim();

        }

        private static bool IsAllowedSiteLabel(string s)
        {
            var m = MapAliasToSite(s);
            return m is "Calcio" or "Pallacanestro" or "Tennis" or "Rugby" or "Football Americano" or "Baseball"
                   || m is "Hockey su ghiaccio"; // <-- ADD
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
     or "RUGBY" or "RUGBY UNION" or "RUGBY LEAGUE"
     or "FOOTBALL AMERICANO" or "AMERICAN FOOTBALL" or "AMERICAN-FOOTBALL" or "NFL"
     or "BASEBALL"
     or "HOCKEY SU GHIACCIO" or "ICE HOCKEY" or "ICE-HOCKEY" or "HOCKEY"; // <-- ADD

        }

        private static List<string> ReorderToStart(List<string> labels, string? startSport)
        {
            if (labels.Count == 0) return labels;

            var preferred = new[] { "Calcio", "Pallacanestro", "Tennis", "Rugby", "Football Americano", "Baseball", "Hockey su ghiaccio" }; // <-- ADD

            string MapAlias(string s)
            {
                var L = s.Trim();
                if (L.Equals("Soccer", StringComparison.OrdinalIgnoreCase) ||
                    L.Equals("Football", StringComparison.OrdinalIgnoreCase)) return "Calcio";
                if (L.Equals("Basketball", StringComparison.OrdinalIgnoreCase)) return "Pallacanestro";
                if (L.StartsWith("Rugby", StringComparison.OrdinalIgnoreCase)) return "Rugby";
                if (L.Contains("AMERICAN", StringComparison.OrdinalIgnoreCase) || L.Contains("FOOTBALL AMERICANO", StringComparison.OrdinalIgnoreCase))
                    return "Football Americano";
                if (L.Equals("Baseball", StringComparison.OrdinalIgnoreCase)) return "Baseball";
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

        private static (string? U, string? O) PickBestOU(Dictionary<string, Dictionary<string, string>>? ou)
        {
            if (ou == null || ou.Count == 0) return (null, null);

            // try numeric sort first
            var numeric = new List<(double line, string? U, string? O)>();
            foreach (var kv in ou)
            {
                var key = (kv.Key ?? "").Replace(',', '.');
                if (double.TryParse(key, NumberStyles.Float, CultureInfo.InvariantCulture, out var num))
                {
                    kv.Value.TryGetValue("UNDER", out var u);
                    kv.Value.TryGetValue("OVER", out var o);
                    numeric.Add((num, u, o));
                }
            }

            if (numeric.Count > 0)
            {
                var best = numeric.OrderBy(x => x.line).First();
                return (best.U, best.O);
            }

            // fallback: first non-numeric entry
            foreach (var kv in ou)
            {
                kv.Value.TryGetValue("UNDER", out var u);
                kv.Value.TryGetValue("OVER", out var o);
                if (!string.IsNullOrWhiteSpace(u) || !string.IsNullOrWhiteSpace(o))
                    return (u, o);
            }

            return (null, null);
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

            return mapped;
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
                "Football Americano" => u.Contains("/scommesse/football-americano") || u.Contains("/scommesse/american-football"),
                "Baseball" => u.Contains("/scommesse/baseball"),
                "Hockey su ghiaccio" => u.Contains("/scommesse/hockey-su-ghiaccio") || u.Contains("/scommesse/ice-hockey") || u.Contains("/hockey"), // <-- ADD
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
                "Football Americano" => AmerFootballHrefKeys,
                "Baseball" => BaseballHrefKeys,
                "Hockey su ghiaccio" => IceHockeyHrefKeys, // <-- ADD
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

                    await arrowButton.WaitForAsync(new() { State = WaitForSelectorState.Attached, Timeout = 10000 });
                    await arrowButton.WaitForAsync(new() { State = WaitForSelectorState.Visible, Timeout = 10000 });

                    if (!await arrowButton.IsEnabledAsync())
                    {
                        Console.WriteLine("Arrow still disabled — skipping this league to avoid a hard timeout.");
                        return;
                    }

                    try
                    {
                        await arrowButton.ScrollIntoViewIfNeededAsync();
                        await arrowButton.HoverAsync();
                        Console.WriteLine("Clicking ▶ arrow...");
                        await arrowButton.ClickAsync();
                    }
                    catch
                    {
                        await page.WaitForTimeoutAsync(500);
                        arrowButton = page.Locator(".widget-sel a:has(i.fas.fa-play)");
                        await arrowButton.WaitForAsync(new() { State = WaitForSelectorState.Visible, Timeout = 5000 });
                        await arrowButton.ClickAsync();
                    }

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
                                Console.WriteLine("Fixtures loaded — pausing.");
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

       private static async Task HandleRandomPopup(IPage page)
{
    // 1) quick win: try ESC
    try { await page.Keyboard.PressAsync("Escape"); } catch { }

    // 2) common consent managers (OneTrust, Quantcast, etc.)
    try
    {
        var known = new[]
        {
            "#onetrust-reject-all-handler", "#onetrust-accept-btn-handler",
            "#qc-cmp2-reject-all", "#qc-cmp2-accept-all",
            "button[aria-label='Close']", ".ot-close-icon", ".ot-pc-close",
            "button[mode='deny']", "button[mode='accept']"
        };
        foreach (var sel in known)
        {
            var b = page.Locator(sel).First;
            if (await b.IsVisibleAsync(new() { Timeout = 400 }))
            {
                await b.ClickAsync(new() { Force = true });
                await page.WaitForTimeoutAsync(200);
            }
        }
    }
    catch { }

    // 3) robust text-based clicks (Italian/English variants)
    async Task ClickByTextAsync(ILocator scope)
    {
        string[] labels =
        {
            "Non ora", "Non adesso", "No grazie",
            "Rifiuta", "Rifiuta tutto",
            "Continua senza accettare", "Continua senza accettare tutto",
            "Chiudi", "Chiudi finestra",
            "Dismiss", "Close", "Not now",
            "Reject", "Reject all",
            "Accept", "Accept all"
        };

        foreach (var txt in labels)
        {
            try
            {
                var btn = scope.GetByRole(AriaRole.Button, new() { Name = txt, Exact = false });
                if (await btn.IsVisibleAsync(new() { Timeout = 300 }))
                {
                    await btn.ClickAsync(new() { Force = true });
                    await page.WaitForTimeoutAsync(150);
                }
            }
            catch { }
        }

        // generic fallbacks
        var guesses = new[]
        {
            "button:has-text('Non')",
            "button:has-text('Rifiuta')",
            "button:has-text('Chiudi')",
            "button:has-text('Close')",
            "button:has-text('Reject')",
            ".kumulos-prompt button",
            ".modal.show .btn, .modal.show button"
        };

        foreach (var sel in guesses)
        {
            try
            {
                var b = scope.Locator(sel).First;
                if (await b.IsVisibleAsync(new() { Timeout = 300 }))
                {
                    await b.ClickAsync(new() { Force = true });
                    await page.WaitForTimeoutAsync(150);
                }
            }
            catch { }
        }
    }

    // 4) main page
await ClickByTextAsync(page.Locator("body"));

    // 5) iframes (some sports load a consent/prompt in an iframe)
    try
    {
        foreach (var f in page.Frames)
        {
            if (f == page.MainFrame) continue;
            try { await ClickByTextAsync(f.Locator("body")); } catch { }
        }
    }
    catch { }

    // 6) overlays specific to that Kumulos prompt you already saw
    try
    {
        var overlay = page.Locator(".kumulos-background-mask, .mfp-bg, .modal-backdrop").First;
        if (await overlay.IsVisibleAsync(new() { Timeout = 400 }))
        {
            try { await overlay.ClickAsync(new() { Force = true }); } catch { }
        }

        var prompt = page.Locator(".kumulos-prompt").First;
        if (await prompt.IsVisibleAsync(new() { Timeout = 400 }))
        {
            var anyBtn = prompt.Locator("button").First;
            if (await anyBtn.IsVisibleAsync(new() { Timeout = 300 }))
                await anyBtn.ClickAsync(new() { Force = true });
        }
    }
    catch { }

    // 7) last-resort nuke (safe selectors only; avoids breaking page content)
    try
    {
        await page.EvaluateAsync(@"
(() => {
  const kill = sel => document.querySelectorAll(sel).forEach(el => { el.remove(); });
  // benign overlays/dialog shells
  kill('.kumulos-background-mask');
  kill('.kumulos-prompt');
  kill('#onetrust-consent-sdk');
  kill('.ot-sdk-container');
  kill('.qc-cmp2-container');
  // generic blocking overlays
  document.querySelectorAll('[role=dialog], .modal.show, .iziToast-overlay')
    .forEach(el => { if (el.getBoundingClientRect().height > 80) el.style.display='none'; });
})();
");
    }
    catch { }

    // small breath after cleanup
    await page.WaitForTimeoutAsync(120);
}

        // =========================
        // Keep a target element reliably in view (USED ONLY FOR AF/BB enrichment)
        // =========================
        private static async Task KeepInViewAsync(IPage page, ILocator target)
        {
            try
            {
                await target.ScrollIntoViewIfNeededAsync();
                await page.WaitForTimeoutAsync(120);
                var handle = await target.ElementHandleAsync();
                if (handle != null)
                {
                    await page.EvaluateAsync(@"(el) => {
                        try { el.scrollIntoView({block:'center', inline:'center', behavior:'instant'}); } catch {}
                    }", handle);
                }
            }
            catch { }
        }
    }
}
