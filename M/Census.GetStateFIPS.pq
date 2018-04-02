let GetStateFIPS = (table as table, geography as text) => // the input table must have the following text column => ("geoid")
    let 
        // get an authoritative table of U.S. states from the U.S. Census Bureau
        // states contain the following text columns => {"Name", "FIPS State Numeric Code", "Official USPS Code"}
        // outlying areas contain the following text columns => {"Area Name", "FIPS State Numeric Code", "Official USPS Code"}
        StatesTable = // create states table
            let
                Source = GetTables("https://www.census.gov/geo/reference/ansi_statetables.html"),
                States = 
                    let
                        Data = Source{0}[Data],
                        SelectedColumns = Table.SelectColumns(Data,{"Name", "FIPS State Numeric Code", "Official USPS Code"}),
                        RenamedColumns = Table.RenameColumns(SelectedColumns,{{"Name", "state_name"}, {"FIPS State Numeric Code", "state_fips"}, {"Official USPS Code", "state_abbr"}})
                    in
                        RenamedColumns,
                PuertoRico = 
                    let
                        Data = Source{1}[Data],
                        SelectedColumns = Table.SelectColumns(Data,{"Area Name", "FIPS State Numeric Code", "Official USPS Code"}),
                        RenamedColumns = Table.RenameColumns(SelectedColumns,{{"Area Name", "state_name"}, {"FIPS State Numeric Code", "state_fips"}, {"Official USPS Code", "state_abbr"}}),
                        SelectedPR = Table.SelectRows(RenamedColumns, each ([state_fips] = "72"))
                    in
                        SelectedPR,
                UnionStates = Table.Combine({States, PuertoRico})
            in
                UnionStates,
        Source = table, //read input table
        AddedStateFIPS = 
            let
                input = 
                    if geography <> "zcta" then 
                        Table.AddColumn(Source, "state_fips", each Text.Start([geoid],2), type text) // splice first two characters of GEOID to create FIPS State Numeric Code
                    else
                        let
                            ZipToStateTable = 
                                let
                                    Source = Csv.Document(Web.Contents("https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt"),[Delimiter=",", Columns=24, Encoding=1252, QuoteStyle=QuoteStyle.None]),
                                    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
                                    SelectedColumns = Table.SelectColumns(PromotedHeaders,{"ZCTA5", "STATE"}),
                                    RenamedColumns = Table.RenameColumns(SelectedColumns,{{"ZCTA5", "geoid"}, {"STATE", "state_fips"}}),
                                    RemovedDuplicates = Table.Distinct(RenamedColumns)
                                in
                                    RemovedDuplicates,
                            ZipCodesTable = 
                                let
                                    ZipTable = table
                                in
                                    ZipTable,
                            JoinedZipCodes = Table.NestedJoin(ZipToStateTable,{"geoid"},ZipCodesTable,{"geoid"},"zip_codes",JoinKind.LeftOuter),
                            RemovedZip = Table.RemoveColumns(JoinedZipCodes,{"geoid"}),
                            ExpandedZipCodes = Table.ExpandTableColumn(RemovedZip, "zip_codes", {"geoid"}, {"geoid"})
                        in
                            ExpandedZipCodes
            in
                input,
        MergedStates = Table.NestedJoin(AddedStateFIPS,{"state_fips"},StatesTable,{"state_fips"},"states",JoinKind.LeftOuter), // merge states table to get state name and state abbreviation
        ExpandedStates = Table.ExpandTableColumn(MergedStates, "states", {"state_name", "state_abbr"}, {"state_name", "state_abbr"})
    in
        ExpandedStates
in
    GetStateFIPS