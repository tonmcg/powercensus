let
    Census.GetStates = (optional includeTerritories as logical) as table =>
        let
            resource = Web.BrowserContents("https://www.census.gov/geo/reference/ansi_statetables.html"),
            States = 
                let 
                    ExtractedTable = Html.Table(resource, {{"state_name", "[scope=""row""]:nth-last-child(3)"}, {"state_fips", "[align=""center""]:nth-last-child(2)"}, {"state_abbr", "[align=""center""]:nth-last-child(1)"}}, [RowSelector="TABLE[summary=""table showing ANSI state codes for the states and the District of Columbia""] > TBODY > TR"])
                in
                    ExtractedTable,
            Territories = 
                let 
                    ExtractedTable = Html.Table(resource, {{"state_name", "[scope=""row""]:nth-last-child(4)"}, {"state_fips", "[align=""center""]:nth-last-child(3)"}, {"state_abbr", "[align=""center""]:nth-last-child(2)"}}, [RowSelector="TABLE[summary=""table showing ANSI state codes for the outlying areas of the United States, the freely associated states, and trust territory""] > TBODY > TR"])
                in
                    ExtractedTable,
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
        Documentation.Description = " Return a table of United States states along with FIPS codes.",
        Documentation.LongDescription = " Return a table of United States states. If the optional parameter includeTerritories is set to TRUE, the returned table will also include information on Outlying Areas of the United States and the Freely Associated States.",
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
    Value.ReplaceType(Census.GetStates, ParameterTypes)