let 
    Census.GetStates = let 
        StatesFunction = (optional includeTerritories as logical) as table =>
        let
			// Html.GetTables function by Tony McGovern
			// for examples, see https://tonymcgovern.com/powerquery/census/get-us-states
			// for source, see https://gist.github.com/tonmcg/1173759b95943b2b9ed290b9edbe74d3
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
        ParameterTypes = type function(
            optional includeTerritories as (
                type logical meta [ 
                    Documentation.AllowedValues = {true, false}
                ]
            )
        ) as table meta            
        [
            Documentation.Name =  " Census.GetStates",
            Documentation.Description = " Return a table of United States states along with FIPS codes. If the optional logical includeTerritories is set to true, the table includes U.S. territories, the Outlying Areas and the Freely Associated States.",
            Documentation.LongDescription = " Return a table of United States states.",
            Documentation.Category = " Table.Transform",
            Documentation.Source = " ",
            Documentation.Author = " Tony McGovern: www.emdata.ai",
            Documentation.Examples = {
                [
                    Description =  "Return a table of United States states, including Puerto Rico and other U.S. territories.", 
                    Code = " GetStates(true)", 
                    Result = " Table.FromRecords({[state_name = ""Alabama"", states_fips = ""01"", state_abbr = ""AL""],[state_name = ""Alaska"", states_fips = ""02"", state_abbr = ""AK""],...,[state_name = ""U.S. Virgin Islands"", states_fips = ""78"", state_abbr = ""VI""]})"
                ]
            }
        ] 
    in 
        Value.ReplaceType(StatesFunction, ParameterTypes)
in
    Census.GetStates