let
	Census.GetGeographicAreas = (year as text, geography as text) as table =>
		let
            returnTable = 
                if geography = "State" then 
                    GetStates(true) 
                else
					let
						geographies = Geographies,
						getGeographyNames = (n as number) =>
							let
								geography = geographies{n}[geography]
							in
								if n = List.Count(geographies) then
									{}
								else
									List.Combine({{geography}, @getGeographyNames(n + 1)}),
						geographyNames = getGeographyNames(0),
						// Congressional Districts follow a different url pattern from other geographies; we'll call these "excepted geographies"
						// test whether the user selected Congressional District as the geography type
						isException = List.Contains({"Congressional District"}, geography),
						// if true, create the url based on the mapping provided in the table below; if false, return null

							/*
							+--------------------------------+--------+---------+
							| GEOGRAPHY                        YEAR     MAPS TO |
							+--------------------------------+--------+---------+
							  Congressional District           2012     112
							  Congressional District           2013     113
							  Congressional District           2014     114
							  Congressional District           2015     114
							  Congressional District           2016     115
							  Congressional District           2017     115
							  Congressional District           2018     116
							+--------------------------------+--------+---------+
							  
							*/

						// Define the Congressional Districts by the meeting that corresponds to the year
						// e.g., 2016 was the 115th meeting of Congress
						CongressionalDistricts = 
							if isException then
								List.First(List.Select(geographies, each _[geography] = "Congressional District"))[yearList]
							else
								null,
						// test whether the user selected an excepted geography
						UrlMap = 
							if isException then CongressionalDistricts else null,
						GeoQuery = 
							if isException then 
								Text.Combine({List.First(List.Select(UrlMap, each Text.From(_[year]) = year))[meeting], "CDs"})
							else 
								List.First(List.Select(geographies, each _[geography] = geography))[gazetteerUrlPart], 
						GeoColumns = List.First(List.Select(geographies, each _[geography] = geography))[numOfColumns],
						GeoName = List.First(List.Select(geographies, each _[geography] = geography))[gazetteerUrlPart],
						// get authoritative list of geographic areas for selected geographic area types 
						GazetteerFile = Csv.Document(UnzipContents(Web.Contents("http://www2.census.gov/geo/docs/maps-data/data/gazetteer/" & year & "_Gazetteer/" & year & "_Gaz_" & GeoQuery & "_national.zip")){0}[Content],[Delimiter="#(tab)", Columns=Expression.Evaluate(Text.From(GeoColumns)), Encoding=1252, QuoteStyle=QuoteStyle.None]),
						PromotedGazetteerHeaders = Table.PromoteHeaders(GazetteerFile, [PromoteAllScalars=true]),
						ColumnSelectList = if List.Contains({"Census Tract", "Congressional District", "ZIP Code Tabulation Area"},geography) then {"GEOID"} else {"GEOID", "NAME"},
						SelectedGazetteerColumns = Table.SelectColumns(PromotedGazetteerHeaders,ColumnSelectList),
						ColumnRenameList = if List.Contains({"Census Tract", "Congressional District", "ZIP Code Tabulation Area"},geography) then {{"GEOID", "geoid"}} else {{"GEOID", "geoid"}, {"NAME", "name"}},
						RenamedGazetteerColumns = Table.RenameColumns(SelectedGazetteerColumns,ColumnRenameList),
						ColumnNamesList = if List.Contains(Table.ColumnNames(RenamedGazetteerColumns),"name") then RenamedGazetteerColumns else Table.AddColumn(RenamedGazetteerColumns, "name", each [geoid], type text),
						DefinedGazetteerColumns = {{"geoid", type text}, {"name", type text}},
						DefinedDataTypes = Table.TransformColumnTypes(ColumnNamesList,DefinedGazetteerColumns),
						GazetteerTable = DefinedDataTypes,
						AddedYear = Table.AddColumn(GazetteerTable, "year", each Number.FromText(year), Int64.Type),
						AddedgeographyType = 
							Table.AddColumn(
								AddedYear, 
								"geography_type", 
								each if isException then 
									Text.Combine({List.First(List.Select(UrlMap, each Text.From(_[year]) = year))[meeting], "th ", geography})
								else
									geography, 
								type text
							),
						AddedIndex = Table.AddIndexColumn(AddedgeographyType, "index", 1, 1)
					in
						AddedIndex
		in
			returnTable,
    ParameterTypes = type function(
        year as (
            type text meta [ 
                Documentation.AllowedValues = {"2012", "2013", "2014", "2015", "2016", "2017","2018" }
            ]
        ),
        geography as (
            type text meta [ 
                Documentation.AllowedValues = {"State", "Urban Area","Core Based Statistical Area","County","County Subdivision","Place","Census Tract","Congressional District","School Districts - Elementary","School Districts - Secondary","School Districts - Unified","ZIP Code Tabulation Area"}
            ]
        )
    ) as table meta        
    [
        Documentation.Name =  " Census.GetGeographicAreas",
        Documentation.Description = " Return a listing of all geographic areas for selected United States geographic area types.",
        Documentation.LongDescription = " Return a listing of all geographic areas for selected United States geographic area types. The year parameter specifies that year's listing of geographic areas. The geography parameter defines the specific geographic type to return. Geographic areas are updated by the U.S. Census Bureau every year. All geographic types from the U.S. Census Gazetteer Files website (https://www.census.gov/geo/maps-data/data/gazetteer.html) are available.",
        Documentation.Category = " Table.Transform",
        Documentation.Source = " ",
        Documentation.Author = " Tony McGovern: www.emdata.ai",
        Documentation.Examples = {
            [
                Description =  "Return a listing of all United States counties as of 2017.", 
                Code = " GetGeographicAreas(""2017"",""County"")", 
                Result = "Table.FromRecords({[geoid = ""01001"",name = ""Autauga County"",year = 2017,geography_type = ""County"",index = 1],[geoid = ""01003"",name = ""Baldwin County"",year = 2017,geography_type = ""County"",index = 2],[geoid = ""01005"",name = ""Barbour County"",year = 2017,geography_type = ""County"",index = 3],[geoid = ""..."",name = ""..."",year = ""..."",geography_type = ""..."",index = ""...""],[geoid = ""72153"",name = ""Yauco Municipio"",year = 2017,geography_type = ""County"",index = 3220]})"
            ]
        }
    ] 
in 
    Value.ReplaceType(Census.GetGeographicAreas, ParameterTypes)