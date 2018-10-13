let
	Census.GetZipRelationships = (geography as text) as table =>
		let
			geographies = Geographies,
			isState = if geography = "State" then true else false,
			geographyTable = 
				if isState then
					let
						intialTable = GetStates(true),
						// filter out outlying territories but keep Puerto Rico
						filteredTable = Table.SelectRows(intialTable, each (not List.Contains(US_Territories,[state_name])))
					in
						filteredTable
				else
					GetGeographicAreas("2018", geography),
			GeoQuery = List.First(List.Select(geographies, each _[geography] = geography))[zipUrlPart],
			GeoColumns = List.First(List.Select(geographies, each _[geography] = geography))[numOfColumns],
			// zip code-to-state relationship table
			relationshipTable = Csv.Document(Web.Contents("https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_" & Text.Lower(GeoQuery) & "_rel_10.txt"),[Delimiter=",", Columns=Expression.Evaluate(Text.From(GeoColumns)), Encoding=1252, QuoteStyle=QuoteStyle.None]),
			PromotedHeaders = Table.PromoteHeaders(relationshipTable, [PromoteAllScalars=true]),
			ColumnSelectList = if isState then {"ZCTA5", "STATE"} else if List.Contains({"County","County Subdivision","Place","Census Tract"},geography) then {"ZCTA5", "GEOID"} else if geography = "Core Based Statistical Area" then {"ZCTA5", "CBSA"} else {"ZCTA5","NECTA"},
			SelectedGazetteerColumns = Table.SelectColumns(PromotedHeaders,ColumnSelectList),
			ColumnRenameList = if isState then {{"ZCTA5", "zcta5"}, {"STATE", "state_fips"}} else if List.Contains({"County","County Subdivision","Place","Census Tract"},geography) then {{"ZCTA5", "zcta5"}, {"GEOID", "geoid"}} else if geography = "Core Based Statistical Area" then {{"ZCTA5", "zcta5"}, {"CBSA", "geoid"}} else {{"ZCTA5", "zcta5"}, {"NECTA", "geoid"}},
			RenamedGazetteerColumns = Table.RenameColumns(SelectedGazetteerColumns,ColumnRenameList),
			MergedQueries = Table.NestedJoin(RenamedGazetteerColumns,if isState then {"state_fips"} else {"geoid"},geographyTable,if isState then {"state_fips"} else {"geoid"},"GetZip",JoinKind.FullOuter),
			ExpandedStates = if isState then Table.ExpandTableColumn(MergedQueries, "GetZip", {"state_name", "state_abbr"}, {"state_name", "state_abbr"}) else Table.ExpandTableColumn(MergedQueries, "GetZip", {"name"}, {"name"}),
			AddedIndex = Table.AddIndexColumn(ExpandedStates, "index", 1, 1)
		in
			AddedIndex,
    ParameterTypes = type function(
        geography as (
            type text meta [ 
                Documentation.AllowedValues = {"State", "Core Based Statistical Area","County","County Subdivision","Place","Census Tract"} // "New England City and Town Area" to be added
            ]
        )
    ) as table meta        
    [
        Documentation.Name =  " Census.GetZipRelationships",
        Documentation.Description = "  Return the relationship between ZIP Code Tabulation Areas  and other geographic entities",
        Documentation.LongDescription = " Return the relationship between ZIP Code Tabulation Areas (ZCTAs) and other geographic entities. The 𝘨𝘦𝘰𝘨𝘳𝘢𝘱𝘩𝘺 parameter defines the specific geographic type whose relationship with ZCTAs will be created. Most geographic types from the U.S. Census ZIP Code Tabulation Area (ZCTA) Relationship Files website (https://www.census.gov/geo/maps-data/data/zcta_rel_download.html) are available.",
        Documentation.Category = " Table.Transform",
        Documentation.Source = " ",
        Documentation.Author = " Tony McGovern: www.emdata.ai",
        Documentation.Examples = {
            [
                Description =  "Return a listing of relationships between ZCTAs and U.S. States.", 
                Code = " GetZipRelationships(""State"")", 
                Result = "Table.FromRecords({[geoid = ""01001"",name = ""Autauga County"",year = 2017,geography_type = ""County"",index = 1],[geoid = ""01003"",name = ""Baldwin County"",year = 2017,geography_type = ""County"",index = 2],[geoid = ""01005"",name = ""Barbour County"",year = 2017,geography_type = ""County"",index = 3],[geoid = ""..."",name = ""..."",year = ""..."",geography_type = ""..."",index = ""...""],[geoid = ""72153"",name = ""Yauco Municipio"",year = 2017,geography_type = ""County"",index = 3220]})"
            ]
        }
    ] 
in 
    Value.ReplaceType(Census.GetZipRelationships, ParameterTypes)