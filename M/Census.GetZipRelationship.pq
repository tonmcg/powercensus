let
    GetZipRelationship = (table as table, geography as text) =>
        let
            JoinGeography = 
                let
                    GeographyToState =
                        let
                            ZipToState =
                                let
                                    // zip code-to-state relationship table
                                    Source = Csv.Document(Web.Contents("https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt"),[Delimiter=",", Columns=2, Encoding=1252, QuoteStyle=QuoteStyle.None]),
                                    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
                                    SelectedGazetteerColumns = Table.SelectColumns(PromotedHeaders,{"ZCTA5", "STATE"}),
                                    RenamedGazetteerColumns = Table.RenameColumns(SelectedGazetteerColumns,{{"ZCTA5", "geoid"}, {"STATE", "state_fips"}}),
                                    RemoveGeographyDuplicates = Table.Distinct(RenamedGazetteerColumns)
                                in
                                    RemoveGeographyDuplicates,
                            ZipToGeography =
                                if geography = "Core Based Statistical Area" then
                                    let
                                        // zip code-to-cbsa relationship joined to zip code-to-state relationship table
                                        Source = Csv.Document(Web.Contents("https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_cbsa_rel_10.txt"),[Delimiter=",", Columns=2, Encoding=1252, QuoteStyle=QuoteStyle.None]),
                                        PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
                                        SelectedGazetteerColumns = Table.SelectColumns(PromotedHeaders,{"ZCTA5", "CBSA"}),
                                        RenamedGazetteerColumns = Table.RenameColumns(SelectedGazetteerColumns,{{"ZCTA5", "geoid"}, {"CBSA", "cbsa"}}),
                                        RemoveGeographyDuplicates = Table.Distinct(RenamedGazetteerColumns),
                                        JoinedStateToGeography = Table.NestedJoin(RemoveGeographyDuplicates,{"geoid"},ZipToState,{"geoid"},"StateToGeography",JoinKind.LeftOuter),
                                        ExpandedStateFIPS = Table.ExpandTableColumn(JoinedStateToGeography, "StateToGeography", {"state_fips"}, {"state_fips"}),
                                        SelectGeographyColumns = Table.SelectColumns(ExpandedStateFIPS,{"cbsa", "state_fips"}),
                                        RemovedDuplicates = Table.Distinct(SelectGeographyColumns),
                                        RenamedGeographyColumns = Table.RenameColumns(RemovedDuplicates,{{"cbsa", "geoid"}})
                                    in
                                        RenamedGeographyColumns
                                else if geography = "Urban Area" then
                                    let
                                        // zip code-to-urban area relationship joined to zip code-to-state relationship table
                                        Source = Csv.Document(Web.Contents("https://www2.census.gov/geo/docs/maps-data/data/rel/ua_zcta_rel_10.txt"),[Delimiter=",", Columns=3, Encoding=1252, QuoteStyle=QuoteStyle.None]),
                                        PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
                                        SelectedGazetteerColumns = Table.SelectColumns(PromotedHeaders,{"ZCTA5", "UA"}),
                                        RenamedGazetteerColumns = Table.RenameColumns(SelectedGazetteerColumns,{{"ZCTA5", "geoid"}, {"UA", "ua"}}),
                                        RemoveGeographyDuplicates = Table.Distinct(RenamedGazetteerColumns),
                                        JoinedStateToGeography = Table.NestedJoin(RemoveGeographyDuplicates,{"geoid"},ZipToState,{"geoid"},"StateToGeography",JoinKind.LeftOuter),
                                        ExpandedStateFIPS = Table.ExpandTableColumn(JoinedStateToGeography, "StateToGeography", {"state_fips"}, {"state_fips"}),
                                        SelectGeographyColumns = Table.SelectColumns(ExpandedStateFIPS,{"ua", "state_fips"}),
                                        RemovedDuplicates = Table.Distinct(SelectGeographyColumns),
                                        RenamedGeographyColumns = Table.RenameColumns(RemovedDuplicates,{{"ua", "geoid"}})
                                    in
                                        RenamedGeographyColumns
                                else
                                    ZipToState
                        in
                            ZipToGeography,
                    Source = Table.NestedJoin(table,{"geoid"},GeographyToState,{"geoid"},"GeographyToState",JoinKind.Inner),
                    ExpandedState = Table.ExpandTableColumn(Source, "GeographyToState", {"state_fips"}, {"state_fips"})
                in
                    ExpandedState
        in
            JoinGeography
in
    GetZipRelationship