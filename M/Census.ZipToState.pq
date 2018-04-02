let
    GetTables = (url as text) =>
    let
        DOM = Text.FromBinary(Web.Contents(url)),
        DOCTYPE = "<!" & Text.BetweenDelimiters(DOM, "<!", ">") & ">",
        HTMLOpeningTag = "<html" & Text.BetweenDelimiters(DOM, "<html", ">") & ">",
        HEAD = "<head" & Text.BetweenDelimiters(DOM, "<head", "</head>") & "</head>",
        BODYOpeningTag = "<body>",
        ScrapeTables = (n as number) =>
            let
                CurrentTable = Text.BetweenDelimiters(DOM, "<table", "</table>", n)
            in
                if CurrentTable = "" then 
                    ""
                else
                    Text.Combine({ "<table", CurrentTable, "</table>", @ScrapeTables(n+1) }),
        TableText = ScrapeTables(0),
        HTML = Text.Combine({DOCTYPE, HTMLOpeningTag, HEAD, BODYOpeningTag, TableText, "</body></html>"}),
        Page = Web.Page(HTML),
        Tables = Table.SelectRows(Page, each ([Source] = "Table"))
    in
        Tables,
    StateTable = GetTables("https://www.census.gov/geo/reference/ansi_statetables.html"),
    GetStates = (includeTerritories as logical) =>
    let
        States = 
            let
                Data = StateTable{0}[Data],
                SelectedGazetteerColumns = Table.SelectColumns(Data,{"Name", "FIPS State Numeric Code", "Official USPS Code"}),
                RenamedGazetteerColumns = Table.RenameColumns(SelectedGazetteerColumns,{{"Name", "state_name"}, {"FIPS State Numeric Code", "state_fips"}, {"Official USPS Code", "state_abbr"}})
            in
                RenamedGazetteerColumns,
        Territories = 
              let
                  Data = StateTable{1}[Data],
                  SelectedGazetteerColumns = Table.SelectColumns(Data,{"Area Name", "FIPS State Numeric Code", "Official USPS Code"}),
                  RenamedGazetteerColumns = Table.RenameColumns(SelectedGazetteerColumns,{{"Area Name", "state_name"}, {"FIPS State Numeric Code", "state_fips"}, {"Official USPS Code", "state_abbr"}})
              in
                  RenamedGazetteerColumns,
        UnionStates = if not includeTerritories or includeTerritories = null then States else Table.Combine({States, Territories})
    in
        UnionStates,
    USStates = GetStates(true),
    // zip code-to-state relationship table
    Source = Csv.Document(Web.Contents("https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt"),[Delimiter=",", Columns=2, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    SelectedGazetteerColumns = Table.SelectColumns(PromotedHeaders,{"ZCTA5", "STATE"}),
    RenamedGazetteerColumns = Table.RenameColumns(SelectedGazetteerColumns,{{"ZCTA5", "zcta5"}, {"STATE", "state_fips"}}),
    AddedIndex = Table.AddIndexColumn(RenamedGazetteerColumns, "index", 1, 1),
    MergedQueries = Table.NestedJoin(AddedIndex,{"state_fips"},USStates,{"state_fips"},"GetStates",JoinKind.LeftOuter),
    ExpandedStates = Table.ExpandTableColumn(MergedQueries, "GetStates", {"state_name", "state_abbr"}, {"state_name", "state_abbr"})
in
    ExpandedStates