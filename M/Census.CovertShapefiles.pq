let
    geography = "county_within_ua",
    Census.GetShapefiles = (state_fips as text, geography as text) =>
        let 
            shapefiles = Web.Contents("http://www2.census.gov/geo/tiger/GENZ2017/shp/cb_2017_" & state_fips & "_" & geography & "_500k.zip")
        in
            shapefiles,
    statesTable = GetGeographicAreas("2018", "State"),
    AddedShapefileName = Table.AddColumn(statesTable, "filename", each "cb_2017_" & [state_fips] & "_" & geography & "_500k.zip", type text),
    FilteredToResultFIPS = Table.SelectRows(AddedShapefileName, each ([state_name] <> "Federated States of Micronesia" and [state_name] <> "Marshall Islands" and [state_name] <> "Palau" and [state_name] <> "U.S. Minor Outlying Islands")),
    FilteredToDC = Table.SelectRows(FilteredToResultFIPS, each ([state_name] = "District of Columbia")),
    getShapefiles = Table.AddColumn(FilteredToDC, geography & "_shapefile", each Census.GetShapefiles([state_fips], geography), type binary),
    AddedShapefileMetadata = Table.AddColumn(getShapefiles, "Metadata", each Value.Metadata([county_within_ua_shapefile]), type record),
    ExpandedShapefileMetadata = Table.ExpandRecordColumn(AddedShapefileMetadata, "Metadata", {"Content.Type", "Content.Uri", "Content.Name", "Headers", "Request.Options", "Response.Status"}, {"Metadata.Content.Type", "Metadata.Content.Uri", "Metadata.Content.Name", "Metadata.Headers", "Metadata.Request.Options", "Metadata.Response.Status"}),
    metadata = ExpandedShapefileMetadata{0},
    shapefileBinary = metadata[county_within_ua_shapefile],
    MetadataRecord = Value.Metadata(shapefileBinary),
    filename = metadata[filename],
    contentType = Value.Metadata(shapefileBinary)[Content.Type]?,
    fileContents = shapefileBinary meta [name="upload", type = contentType],
    parts = [
        file = fileContents,
        locale = "en-US"
    ],    
    url = "https://ogre.adc4gis.com/convert",
    Web.MultiPartPost = (url as text, parts as record) as binary =>
        let
            unique = Text.NewGuid(),
            boundary = "--" & unique,
            crlf = "#(cr)#(lf)",
            item = (name, value) =>
                let
                    filename = Value.Metadata(value)[name]?,
                    contentType = Value.Metadata(value)[type]?,
                    line1 = "Content-Disposition: form-data; name=""" & name & """" &
                        (if filename = null then "" else "; filename=""" & filename & """"),
                    line2 = if contentType = null then {} else { "Content-Type: " & contentType },
                    lines = { boundary, line1 } & line2 & { "", value }
                in
                    Text.Combine(lines, crlf) & crlf,
            body = Text.Combine(List.Transform(Record.FieldNames(parts), each item(_, Record.Field(parts, _)))) & boundary & "--" & crlf
        in
            Web.Contents(url, [
                Headers=[#"Content-Type"="multipart/form-data; boundary=" & unique],
                Content=Text.ToBinary(body)
            ]),
    Result = Web.MultiPartPost(url, parts)
in
    Result