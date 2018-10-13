let
    Census.FixGEOID = (tbl as table, columnName as text, geography as text) as table =>
		let
			geoIdLength = if geography = "State" then 2 else if geography = "County" then 3 else if geography = "Census Tract" then 6 else null,
			// get column data type
			columnType = Table.Schema(tbl){[Name = columnName]}[Kind],
			// test column data type
			isText = if columnType = "text" then true else false,
			// if not text, change to text
			ChangedDataType = if isText then tbl else Table.TransformColumnTypes(tbl,{{columnName, type text}}), 
			// test each text length: if less than n characters long, then prepend the appropriate amount of "0", else leave as is
			AddedPrefix = Table.TransformColumns(ChangedDataType, {{columnName, each if Text.Length(_) < geoIdLength then Text.Repeat("0", geoIdLength - Text.Length(_)) & _ else _, type text}})
		in
			AddedPrefix,
	ParameterTypes = type function(
		tbl as table,
		columnName as text,
		geography as (
			type text meta [ 
				Documentation.AllowedValues = {"State", "Urban Area","Core Based Statistical Area","County","County Subdivision","Place","Census Tract","Congressional District","School Districts - Elementary","School Districts - Secondary","School Districts - Unified","ZIP Code Tabulation Area"}
			]
		)
	) as table meta
	[
		Documentation.Name =  " Census.FixGEOID",
		Documentation.Description = " Returns the full geographic identifier code for selected United States geographic area type.",
		Documentation.LongDescription = " Many U.S. geographic identifier codes begin with a leading zero (0). Some spreadsheet-based applications, including Microsfot Excel, automatically convert character strings that begin with zero as a numeric data type and strip the leading digit from the identifier. This function restores the leading zeroes to create the full geographic identifier code for selected United States geographic area type. The 𝘵𝘣𝘭 parameter accepts a table as input. The 𝘤𝘰𝘭𝘶𝘮𝘯𝘕𝘢𝘮𝘦 parameter is the name of the column that contains stripped geographic identifier codes. The 𝘨𝘦𝘰𝘨𝘳𝘢𝘱𝘩𝘺 parameter defines the specific geographic type whose identifier code requires restoring. Any geographic type from the U.S. Census Gazetteer Files website (https://www.census.gov/geo/maps-data/data/gazetteer.html) can be used for the 𝘨𝘦𝘰𝘨𝘳𝘢𝘱𝘩𝘺 parameter.",
		Documentation.Category = " Table.Transform",
		Documentation.Source = " ",
		Documentation.Author = " Tony McGovern: www.emdata.ai",
		Documentation.Examples = {
			[
				Description =  "Return the correct identifier code for select United States states.", 
				Code = " FixGEOID(Table.FromRecords({[state_code = ""1"", state = ""Alabama""],[state_code = ""2"", state = ""Alaska""],[state_code = ""4"", state = ""Arizona""],[state_code = ""5"", state = ""Arkansas""],[state_code = ""6"", state = ""California""]}), ""state_code"", ""State"")", 
				Result = "Table.FromRecords({[state_code = ""01"", state = ""Alabama""],[state_code = ""02"", state = ""Alaska""],[state_code = ""04"", state = ""Arizona""],[state_code = ""05"", state = ""Arkansas""],[state_code = ""06"", state = ""California""]})"
			]
		}
	] 
in 
    Value.ReplaceType(Census.FixGEOID, ParameterTypes)