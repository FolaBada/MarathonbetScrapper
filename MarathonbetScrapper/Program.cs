// Program.cs
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Playwright;

namespace MarathonbetScrapper
{
    internal static class Program
    {
        // Default order; you can override via: dotnet run -- "Calcio,Tennis,ice-hockey"
        private static readonly string[] DefaultSports = new[]
        {
            "american-football", "ice-hockey", "baseball", "Tennis", "Calcio", "Pallacanestro", "rugby"
        };

        public static async Task Main(string[] args)
        {
            // Resolve sports list (comma-separated arg) or use defaults
            var sports = (args?.Length > 0 && !string.IsNullOrWhiteSpace(args[0]))
                ? args[0].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                : DefaultSports;

            Console.WriteLine("[Startup] Sports queue: " + string.Join(", ", sports));
            Console.WriteLine("Press Ctrl+C to stop.\n");

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;           // don't kill the process immediately
                cts.Cancel();              // signal graceful shutdown
                Console.WriteLine("\n[Shutdown] Ctrl+C received. Finishing current step…");
            };

            using var playwright = await Playwright.CreateAsync();
            var browser = await playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
            {
                Headless = false,  // set true if you want it hidden
                SlowMo = 0,
                Devtools = false
            });

            try
            {
                while (!cts.IsCancellationRequested)
                {
                    foreach (var sport in sports)
                    {
                        if (cts.IsCancellationRequested) break;

                        Console.WriteLine($"\n==== [{DateTime.Now:HH:mm:ss}] START {sport} ====");

                        await using var context = await browser.NewContextAsync();
                        var page = await context.NewPageAsync();

                        try
                        {
                            // IMPORTANT: this method should NOT launch/close the browser,
                            // it should just use the provided page.
                            await MarathonbetScraper.RunSinglePageAsync(page, sport, allowDirectFallback: true);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] while scraping {sport}: {ex.Message}");
                        }
                        finally
                        {
                            await context.CloseAsync();
                        }

                        Console.WriteLine($"==== [{DateTime.Now:HH:mm:ss}] END {sport} ====\n");
                    }

                    // Optional small pause between full cycles (uncomment if you want one)
                    // await Task.Delay(TimeSpan.FromSeconds(10), cts.Token);
                }
            }
            finally
            {
                await browser.CloseAsync();
                Console.WriteLine("[Shutdown] Browser closed. Bye!");
            }
        }
    }
}






// using Microsoft.Playwright;
// using System.Text.Json;
// using System.IO;
// using System.Text;
// using System.Text.RegularExpressions;

// namespace MarathonbetScrapper
// {
//     public static class MarathonbetScraper
//     {
//         public static async Task RunAsync()
//         {
//             Console.WriteLine("Launching browser...");

//             var playwright = await Playwright.CreateAsync();
//             var browser = await playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
//             {
//                 Headless = false,
//                 SlowMo = 1000,
//                 Devtools = true
//             });

//             var context = await browser.NewContextAsync();
//             var page = await context.NewPageAsync();

//             Console.WriteLine("Navigating to Marathonbet...");
//             await page.GotoAsync("https://www.marathonbet.it/scommesse", new() { WaitUntil = WaitUntilState.NetworkIdle });

//             await HandleRandomPopup(page);


//             var sports = await GetSports(page);

//             foreach (var sport in sports)
//             {
//                 Console.WriteLine($"\n=== Processing sport: {sport} ===");

//                 await ClickSportSection(page, sport);
//                 await HandleRandomPopup(page);

//                 // Get leagues dynamically for this sport
//                 var leagues = await GetLeaguesForSport(page);

//                 foreach (var league in leagues)
//                 {
//                     Console.WriteLine($"\n--- Processing league: {league} ---");

//                     await HandleRandomPopup(page);
//                     await ClickLeagueAndLoadOdds(page, league);
//                     await HandleRandomPopup(page);

//                     // Extract odds
//                     await ExtractOddsAsync(page, $"{sport}_{league.Replace(" ", "_")}.json");

//                     // Go back and click the same sport again for the next league
//                     await page.GotoAsync("https://www.marathonbet.it/scommesse", new() { WaitUntil = WaitUntilState.NetworkIdle });
//                     await ClickSportSection(page, sport);
//                 }
//             }
//         }


//         private static async Task<List<string>> GetSports(IPage page)
//         {
//             var sportsElements = page.Locator(".card.elemento-competizioni-widget .titolo-accordion a");
//             int count = await sportsElements.CountAsync();
//             var sports = new List<string>();

//             for (int i = 0; i < count; i++)
//             {
//                 string sportName = (await sportsElements.Nth(i).InnerTextAsync()).Trim();
//                 if (!string.IsNullOrEmpty(sportName))
//                     sports.Add(sportName);
//             }

//             return sports;
//         }

//        private static async Task ClickSportSection(IPage page, string sportName)
// {
//     Console.WriteLine($"=== Processing sport: {sportName.ToUpper()} ===");
//     Console.WriteLine($"Waiting for '{sportName}' section...");

//     try
//     {
//         // Get all sport links
//         var sportLinks = page.Locator(".card.elemento-competizioni-widget .titolo-accordion a");
//         int count = await sportLinks.CountAsync();

//         bool sportFound = false;

//         for (int i = 0; i < count; i++)
//         {
//             var link = sportLinks.Nth(i);
//             string text = (await link.InnerTextAsync()).Trim();

//             if (text.Equals(sportName, StringComparison.OrdinalIgnoreCase))
//             {
//                 Console.WriteLine($"Clicking on '{sportName}' section...");
//                 await HandleRandomPopup(page); // Ensure no popup blocks the click
//                 await link.ClickAsync();
//                 await page.WaitForTimeoutAsync(2000);
//                 sportFound = true;
//                 break;
//             }
//         }

//         if (!sportFound)
//         {
//             Console.WriteLine($"'{sportName}' section not found!");
//         }
//     }
//     catch (Exception ex)
//     {
//         Console.WriteLine($"Error clicking on sport section '{sportName}': {ex.Message}");
//     }
// }

//         private static async Task<List<string>> GetLeaguesForSport(IPage page)
//         {
//             var leagueElements = page.Locator(".box-paese .nome-paese");
//             int count = await leagueElements.CountAsync();
//             var leagues = new List<string>();

//             for (int i = 0; i < count; i++)
//             {
//                 string leagueName = (await leagueElements.Nth(i).InnerTextAsync()).Trim();
//                 if (!string.IsNullOrEmpty(leagueName))
//                     leagues.Add(leagueName);
//             }

//             return leagues;
//         }

//         private static async Task ClickLeagueAndLoadOdds(IPage page, string leagueName)
//         {
//             Console.WriteLine($"Clicking '{leagueName}' and loading fixtures...");

//             var leagueContainer = page.Locator(".box-paese", new PageLocatorOptions { HasTextString = leagueName }).First;

//             if (await leagueContainer.IsVisibleAsync())
//             {
//                 await HandleRandomPopup(page);

//                 var leagueButton = leagueContainer.Locator(".nome-paese");
//                 Console.WriteLine($"Clicking '{leagueName}'...");
//                 await leagueButton.ClickAsync();
//                 await page.WaitForTimeoutAsync(1000);

//                 var arrowButton = page.Locator(".widget-sel a:has(i.fas.fa-play)");

//                 if (await arrowButton.IsVisibleAsync())
//                 {
//                     await HandleRandomPopup(page);

//                     for (int i = 0; i < 10; i++)
//                     {
//                         var isDisabled = await arrowButton.GetAttributeAsync("href-disabled");
//                         Console.WriteLine($"Arrow state: {(isDisabled == null ? "enabled" : "disabled")}");
//                         if (isDisabled == null) break;
//                         await page.WaitForTimeoutAsync(500);
//                     }

//                     await arrowButton.ScrollIntoViewIfNeededAsync();
//                     await arrowButton.HoverAsync();
//                     Console.WriteLine("Clicking ▶ arrow...");
//                     await arrowButton.ClickAsync();

//                     bool fixturesLoaded = false;
//                     for (int i = 0; i < 10; i++)
//                     {
//                         var matchCount = await page.Locator(".tabellaQuoteSquadre").CountAsync();
//                         if (matchCount > 0)
//                         {
//                             fixturesLoaded = true;
//                             Console.WriteLine($"Fixtures loaded: {matchCount} matches found.");
//                             break;
//                         }

//                         Console.WriteLine("Fixtures not detected yet, retrying...");
//                         await page.WaitForTimeoutAsync(2000);
//                     }

//                     if (!fixturesLoaded)
//                         Console.WriteLine("⚠ Fixtures did not load after multiple retries, but continuing...");
//                 }
//                 else
//                 {
//                     Console.WriteLine("▶ play-arrow not found inside widget-sel.");
//                 }
//             }
//             else
//             {
//                 Console.WriteLine($"'{leagueName}' section not found!");
//             }
//         }




//         private class MatchData
//         {
//             public string Teams { get; set; }
//             public Dictionary<string, string> Odds { get; set; }
//         }


//         private static async Task ExtractOddsAsync(IPage page, string fileName)
//         {
//             Console.WriteLine("Extracting odds...");

//             var matchesList = new List<MatchData>();

//             var matchContainers = page.Locator(".tabellaQuoteSquadre");
//             int matchCount = await matchContainers.CountAsync();
//             Console.WriteLine("Fixtures loading...");

//             int validMatchCount = 0;

//             for (int i = 0; i < matchCount; i++)
//             {
//                 var match = matchContainers.Nth(i);

//                 // Scroll container for visibility
//                 try
//                 {
//                     await page.EvaluateAsync(@"(index) => {
//                 const container = document.querySelector('#primo-blocco-sport');
//                 const matches = container?.querySelectorAll('.tabellaQuoteSquadre');
//                 if (matches && matches[index]) {
//                     matches[index].scrollIntoView({behavior: 'instant', block: 'center'});
//                 }
//             }", i);
//                     await page.WaitForTimeoutAsync(500);
//                 }
//                 catch
//                 {
//                     Console.WriteLine($"⚠ Could not scroll match {i}, continuing...");
//                 }

//                 string teamNames = "Unknown Teams";

//                 // Extract teams
//                 var teamElements = await match.Locator("p.font-weight-bold").AllInnerTextsAsync();
//                 var teams = teamElements.Where(t => !string.IsNullOrWhiteSpace(t)).Distinct().Take(2).ToList();

//                 // Fallback to <a> if missing
//                 var anchor = match.Locator("a");
//                 if (await anchor.CountAsync() > 0)
//                 {
//                     try
//                     {
//                         await anchor.First.ScrollIntoViewIfNeededAsync();
//                         var href = await anchor.First.GetAttributeAsync("href");
//                         if (!string.IsNullOrWhiteSpace(href) && href.Contains("vs"))
//                         {
//                             var parts = href.Split('/')
//                                             .Last()
//                                             .Replace("-", " ")
//                                             .Split(new[] { "vs" }, StringSplitOptions.RemoveEmptyEntries)
//                                             .Select(t => t.Trim())
//                                             .Where(t => !string.IsNullOrWhiteSpace(t))
//                                             .ToList();

//                             if (parts.Count == 2)
//                                 teams = parts;
//                         }
//                     }
//                     catch
//                     {
//                         Console.WriteLine("⚠ Could not extract teams from <a>.");
//                     }
//                 }

//                 // Skip unknown matches
//                 if (teams.Count != 2)
//                     continue;

//                 teamNames = $"{teams[0]} vs {teams[1]}";
//                 validMatchCount++;
//                 Console.WriteLine($"\nProcessing match: {teamNames}");

//                 // Locate odds grid for this match
//                 var oddsGrid = match.Locator("xpath=ancestor::div[contains(@class,'contenitoreRiga')]//div[contains(@class,'tabellaQuoteNew')]").First;
//                 bool gridVisible = false;
//                 for (int retry = 0; retry < 5; retry++)
//                 {
//                     if (await oddsGrid.IsVisibleAsync())
//                     {
//                         gridVisible = true;
//                         break;
//                     }
//                     Console.WriteLine($"Odds grid not visible for {teamNames}, retry {retry + 1}...");
//                     await page.WaitForTimeoutAsync(500);
//                 }

//                 if (!gridVisible)
//                 {
//                     Console.WriteLine($"⚠ Odds not found for {teamNames} or match is Live");
//                     continue;
//                 }

//                 // Extract odds for this match only
//                 var oddsElements = oddsGrid.Locator(".contenitoreSingolaQuota");
//                 int oddsCount = await oddsElements.CountAsync();
//                 var odds = new Dictionary<string, string>();

//                 for (int j = 0; j < oddsCount; j++)
//                 {
//                     var oddElement = oddsElements.Nth(j);
//                     try
//                     {
//                         string label = (await oddElement.Locator(".titoloQuotazione").InnerTextAsync()).Trim();
//                         string value = (await oddElement.Locator(".tipoQuotazione_1").InnerTextAsync()).Trim();

//                         if (!string.IsNullOrWhiteSpace(label) && !string.IsNullOrWhiteSpace(value))
//                         {
//                             odds[label] = value;
//                         }
//                     }
//                     catch { }
//                 }

//                 matchesList.Add(new MatchData { Teams = teamNames, Odds = odds });

//                 Console.WriteLine($"Match: {teamNames}");
//                 Console.WriteLine("Odds: " + (odds.Count > 0
//                     ? string.Join(" | ", odds.Select(o => $"{o.Key}: {o.Value}"))
//                     : "No odds available"));
//             }

//             Console.WriteLine($"\n Fixtures loaded: {validMatchCount} matches found.");

//             // Export results to file
//             string jsonOutput = JsonSerializer.Serialize(matchesList, new JsonSerializerOptions { WriteIndented = true });
//             await File.WriteAllTextAsync(fileName, jsonOutput);
//             Console.WriteLine($"\n Odds exported to {fileName}");
//         }


//         private static async Task HandleRandomPopup(IPage page)
//         {
//             try
//             {
//                 for (int retry = 0; retry < 5; retry++)
//                 {
//                     var overlay = page.Locator(".kumulos-background-mask").First;
//                     var prompt = page.Locator(".kumulos-prompt").First;

//                     bool isVisible = false;
//                     try { isVisible = await overlay.IsVisibleAsync(new() { Timeout = 500 }); } catch { }
//                     if (!isVisible)
//                     {
//                         try { isVisible = await prompt.IsVisibleAsync(new() { Timeout = 500 }); } catch { }
//                     }

//                     if (!isVisible)
//                         return;

//                     Console.WriteLine("Popup detected. Attempting to dismiss...");

//                     // Try "Non ora"
//                     var dismissButton = page.GetByText("Non ora");
//                     if (await dismissButton.IsVisibleAsync(new() { Timeout = 500 }))
//                     {
//                         await dismissButton.ClickAsync(new() { Force = true });
//                         Console.WriteLine("Clicked 'Non ora' dismiss button.");
//                     }
//                     else
//                     {
//                         // Try any button inside popup
//                         var promptButton = prompt.Locator("button").First;
//                         if (await promptButton.IsVisibleAsync(new() { Timeout = 500 }))
//                         {
//                             await promptButton.ClickAsync(new() { Force = true });
//                             Console.WriteLine("Clicked popup button.");
//                         }
//                         else
//                         {
//                             // Fallback: click overlay
//                             Console.WriteLine("No dismiss button found. Clicking overlay...");
//                             await overlay.ClickAsync(new() { Force = true });
//                         }
//                     }

//                     try { await overlay.WaitForAsync(new() { State = WaitForSelectorState.Detached, Timeout = 3000 }); } catch { }
//                     try { await prompt.WaitForAsync(new() { State = WaitForSelectorState.Detached, Timeout = 3000 }); } catch { }

//                     await page.WaitForTimeoutAsync(500);
//                 }
//             }
//             catch (Exception ex)
//             {
//                 Console.WriteLine($"Popup handler error: {ex.Message}");
//             }
//         }

//     }


// }

