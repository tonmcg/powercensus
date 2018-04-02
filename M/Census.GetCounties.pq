let
	year = Text.From(Year),
	Source = Csv.Document(UnZip(Web.Contents("http://www2.census.gov/geo/docs/maps-data/data/gazetteer/" & year & "_Gazetteer/" & year & "_Gaz_counties_national.zip")){0}[Content],[Delimiter="#(tab)", Columns=4, Encoding=1252, QuoteStyle=QuoteStyle.None]),
	PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
	SelectedColumns = Table.SelectColumns(PromotedHeaders,{"GEOID", "NAME"}),
	RenamedColumns = Table.RenameColumns(SelectedColumns,{{"GEOID", "geoid"}, {"NAME", "name"}}),
	DefinedDataTypes = Table.TransformColumnTypes(RenamedColumns,{{"geoid", type text}, {"name", type text}}),
	AddedYear = Table.AddColumn(DefinedDataTypes, "year", each year, Int64.Type),
	CountyTable = 
		let
			Counties = GetStateCodes(AddedYear)
		in
			Counties,
	StateTable = 
		let
			States = states
		in
			States,
	InnerJoinedStates = Table.NestedJoin(CountyTable,{"state_fips"},StateTable,{"state_fips"},"states",JoinKind.Inner),
	ExpandedStates = Table.ExpandTableColumn(InnerJoinedStates, "states", {"state_name", "state_abbr"}, {"state_name", "state_abbr"}),
	CreatedPlace = Table.AddColumn(ExpandedStates, "county", each Text.Combine({[name],[state_abbr]},", "), type text),
	AddedIndex = Table.AddIndexColumn(CreatedPlace, "index", 1, 1),
	OrderedCountyTable = Table.FromColumns({AddedIndex[index], AddedIndex[geoid],AddedIndex[name],AddedIndex[county],AddedIndex[state_fips],AddedIndex[state_name],AddedIndex[state_abbr], AddedIndex[year]}, {"index","geoid","name","County","state_fips","state_name","state_abbr","year"})
in
	OrderedCountyTable