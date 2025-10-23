public class BettingMatch
{
    public string Competition { get; set; } = "";
    public string Date { get; set; } = "";
    public string Team1 { get; set; } = "";
    public string Team2 { get; set; } = "";
    public Dictionary<string, string> Odds { get; set; } = new();
}
