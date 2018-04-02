let 
    Census.GetGeographies = let 
        GeographiesFunction = (year as text, geography as text) => 
        let
            // taken from Mark White's blog
            // for examples, see https://tonymcgovern.com/powerquery/unzip-file/
            // for source, see http://sql10.blogspot.sg/2016/06/reading-zip-files-in-powerquery-m.html
            UnZip = (ZIPFile) => 
                let
                    Header = BinaryFormat.Record([
                        MiscHeader = BinaryFormat.Binary(14),
                        BinarySize = BinaryFormat.ByteOrder(BinaryFormat.UnsignedInteger32, ByteOrder.LittleEndian),
                        FileSize   = BinaryFormat.ByteOrder(BinaryFormat.UnsignedInteger32, ByteOrder.LittleEndian),
                        FileNameLen= BinaryFormat.ByteOrder(BinaryFormat.UnsignedInteger16, ByteOrder.LittleEndian),
                        ExtrasLen  = BinaryFormat.ByteOrder(BinaryFormat.UnsignedInteger16, ByteOrder.LittleEndian)    
                    ]),

                    HeaderChoice = BinaryFormat.Choice(
                        BinaryFormat.ByteOrder(BinaryFormat.UnsignedInteger32, ByteOrder.LittleEndian),
                        each if _ <> 67324752             // not the IsValid number? then return a dummy formatter
                            then BinaryFormat.Record([IsValid = false, Filename=null, Content=null])
                            else BinaryFormat.Choice(
                                    BinaryFormat.Binary(26),      // Header payload - 14+4+4+2+2
                                    each BinaryFormat.Record([
                                        IsValid  = true,
                                        Filename = BinaryFormat.Text(Header(_)[FileNameLen]), 
                                        Extras   = BinaryFormat.Text(Header(_)[ExtrasLen]), 
                                        Content  = BinaryFormat.Transform(
                                            BinaryFormat.Binary(Header(_)[BinarySize]),
                                            (x) => try Binary.Buffer(Binary.Decompress(x, Compression.Deflate)) otherwise null
                                        )
                                        ]),
                                        type binary                   // enable streaming
                                )
                    ),

                    ZipFormat = BinaryFormat.List(HeaderChoice, each _[IsValid] = true),

                    Entries = List.Transform(
                        List.RemoveLastN( ZipFormat(ZIPFile), 1),
                        (e) => [FileName = e[Filename], Content = e[Content] ]
                    )
                in
                    Table.FromRecords(Entries),
            geographies = {{"Urban Area","ua","2","UA"},{"Core Based Statistical Area","cbsa","3","CBSA"},{"County","counties","4","County"},{"County Subdivision","cousubs","4","Subdivision"},{"Place","place","4","Place"},{"Census Tract","tracts","2","Census Tract"},{"Congressional District","115CDs","2","Congressional District"},{"ZIP Code Tabulation Area","zcta","1","ZIP Code Tabulation Area"}},
            // if 'geographies' variable matches text in index {0}, return text in index {n}
            GeoQuery = List.First(List.Select(geographies, each _{0} = geography)){1}, 
            GeoColumns = List.First(List.Select(geographies, each _{0} = geography)){2},
            GeoName = List.First(List.Select(geographies, each _{0} = geography)){3},
            // get authoritative list of geographic areas for selected geographic area types 
            GazetteerFile = Csv.Document(UnZip(Web.Contents("http://www2.census.gov/geo/docs/maps-data/data/gazetteer/" & year & "_Gazetteer/" & year & "_Gaz_" & GeoQuery & "_national.zip")){0}[Content],[Delimiter="#(tab)", Columns=Expression.Evaluate(Text.From(GeoColumns)), Encoding=1252, QuoteStyle=QuoteStyle.None]),
            PromotedGazetteerHeaders = Table.PromoteHeaders(GazetteerFile, [PromoteAllScalars=true]),
            ColumnSelectList = if List.Contains({"Census Tract", "Congressional District", "ZIP Code Tabulation Area"},geography) then {"GEOID"} else {"GEOID", "NAME"},
            SelectedGazetteerColumns = Table.SelectColumns(PromotedGazetteerHeaders,ColumnSelectList),
            ColumnRenameList = if List.Contains({"Census Tract", "Congressional District", "ZIP Code Tabulation Area"},geography) then {{"GEOID", "geoid"}} else {{"GEOID", "geoid"}, {"NAME", "name"}},
            RenamedGazetteerColumns = Table.RenameColumns(SelectedGazetteerColumns,ColumnRenameList),
            ColumnNamesList = if List.Contains(Table.ColumnNames(RenamedGazetteerColumns),"name") then RenamedGazetteerColumns else Table.AddColumn(RenamedGazetteerColumns, "name", each [geoid], type text),
            DefinedGazetteerColumns = {{"geoid", type text}, {"name", type text}},
            DefinedDataTypes = Table.TransformColumnTypes(ColumnNamesList,DefinedGazetteerColumns),
            GazetteerTable = DefinedDataTypes,
            //AddedStateFIPS = GetStateFIPS(GazetteerTable, geography),
            AddedYear = Table.AddColumn(GazetteerTable, "year", each Number.FromText(year), Int64.Type),
            AddedgeographyType = Table.AddColumn(AddedYear, "geography_type", each geography, type text),
            AddedIndex = Table.AddIndexColumn(AddedgeographyType, "index", 1, 1)
        in
            AddedIndex,
        ParameterTypes = type function(
            year as (
                type text meta [ 
                    Documentation.AllowedValues = {"2012", "2013", "2014", "2015", "2016", "2017"}
                ]
            ),
            geography as (
                type text meta [ 
                    Documentation.AllowedValues = {"Urban Area", "Core Based Statistical Area","County","County Subdivision","Place","Census Tract","Congressional District","ZIP Code Tabulation Area"}
                ]
            )
        ) as table meta        
        [
            Documentation.Name =  " Census.GetGeographies",
            Documentation.Description = " Return a listing of all geographic areas for selected United States geographic area types. Currently the following geographic types are available: Urban Area, Core Based Statistical Area, County, County Subdivision, Place, Census Tract, Congressional District, and ZIP Code Tabulation Area",
            Documentation.LongDescription = " Return a listing of all geographic areas for selected United States geographic area types from the U.S. Gazetteer Files. The year parameter specifies that year's listing of geographic areas. The geography parameter defines the specific geographic type to return. Geographic areas are updated by the U.S. Census Bureau every year. Currently the following geographic types are available: Urban Area, Core Based Statistical Area, County, County Subdivision, Place, Census Tract, Congressional District, and ZIP Code Tabulation Area.",
            Documentation.Category = " Table.Transform",
            Documentation.Source = " ",
            Documentation.Author = " Tony McGovern: www.emdata.ai",
            Documentation.Examples = {
                [
                    Description =  "Return a listing of all United States counties as of 2017.", 
                    Code = " GetGeographies(""2017"",""County"")", 
                    Result = "Table.FromRecords({[geoid = ""01001"",name = ""Autauga County"",year = 2017,geography_type = ""County"",index = 1],[geoid = ""01003"",name = ""Baldwin County"",year = 2017,geography_type = ""County"",index = 2],[geoid = ""01005"",name = ""Barbour County"",year = 2017,geography_type = ""County"",index = 3],[geoid = ""..."",name = ""..."",year = ""..."",geography_type = ""..."",index = ""...""],[geoid = ""72153"",name = ""Yauco Municipio"",year = 2017,geography_type = ""County"",index = 3220]})"
                ]
            }
        ] 
    in 
        Value.ReplaceType(GeographiesFunction, ParameterTypes)
in
    Census.GetGeographies