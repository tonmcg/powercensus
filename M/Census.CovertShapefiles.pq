let
    geography = "county_within_ua",
    Census.GetShapefiles = Expression.Evaluate(Text.FromBinary(Web.Contents("https://raw.githubusercontent.com/tonmcg/powerCensus/master/M/Census.GetShapefiles.pq")), #shared),
    Census.GetGeographicAreas = Expression.Evaluate(Text.FromBinary(Web.Contents("https://raw.githubusercontent.com/tonmcg/powerCensus/master/M/Census.GetGeographicAreas.pq")), #shared),
    statesTable = Census.GetGeographicAreas("2018", "State"),
    FilteredToResultFIPS = Table.SelectRows(statesTable, each ([state_name] <> "Federated States of Micronesia" and [state_name] <> "Marshall Islands" and [state_name] <> "Palau" and [state_name] <> "U.S. Minor Outlying Islands")),
    AddedShapefileName = Table.AddColumn(FilteredToResultFIPS, "filename", each "cb_2017_" & [state_fips] & "_" & geography & "_500k.zip", type text),
    FilteredToDC = Table.SelectRows(AddedShapefileName, each ([state_name] = "District of Columbia")),
    getShapefiles = Table.AddColumn(FilteredToDC, geography & "_shapefile", each Census.GetShapefiles([state_fips], geography), type binary),
    AddedShapefileMetadata = Table.AddColumn(getShapefiles, "Metadata", (r)=> Value.Metadata(Record.Field(r, geography & "_shapefile")), type record),
    ExpandedShapefileMetadata = Table.ExpandRecordColumn(AddedShapefileMetadata, "Metadata", {"Content.Type", "Content.Uri", "Content.Name", "Headers", "Request.Options", "Response.Status"}, {"Metadata.Content.Type", "Metadata.Content.Uri", "Metadata.Content.Name", "Metadata.Headers", "Metadata.Request.Options", "Metadata.Response.Status"}),
    metadata = ExpandedShapefileMetadata{0},
    shapefileBinary = Record.Field(metadata, geography & "_shapefile"),
    MetadataRecord = Value.Metadata(shapefileBinary),
    file_name = metadata[filename],
    content_type = Value.Metadata(shapefileBinary)[Content.Type]?,
    value = Text.FromBinary(shapefileBinary, 1252) meta [name = "upload", filename = file_name, #"Content-Type" = content_type],
    unique = Text.NewGuid(),
    boundary = "--" & unique,
    crlf = "#(cr)#(lf)",
    name = Value.Metadata(value)[name]?,
    filename = Value.Metadata(value)[filename]?,
    contentType = Value.Metadata(value)[#"Content-Type"]?,
    line1 = "Content-Disposition: form-data; name=""" & name & """" &
        (if filename = null then "" else "; filename=""" & filename & """"),
    line2 = if contentType = null then {} else { "Content-Type: " & contentType },
    line3 = { boundary, "Content-Disposition: form-data; name=""callback""", crlf },
    line4 = { boundary, "Content-Disposition: form-data; name=""sourceSrs""", crlf },
    line5 = { boundary, "Content-Disposition: form-data; name=""targetSrs""", crlf },
    line6 = { boundary, "Content-Disposition: form-data; name=""convert""", crlf },
    lastLine = Text.Combine({boundary, "--"}),
    lines = { boundary, line1 } & line2 & { "", value } & line3 & line4 & line5 & line6 & { lastLine, "" },
    body = Text.Combine(lines, crlf),
    contentLength = Text.Length(body),
    url = "https://ogre.adc4gis.com/convert",

    request = Web.Contents(
        url, 
        [
            Headers = 
            [
                #"Accept-Language"="en-US,en;q=0.9", 
                #"Accept-Encoding"="gzip, deflate", 
                Accept="text/html,application/xhtml+xml,application/xml",
                #"Content-Type" = "multipart/form-data; boundary=" & unique
            ], 
            Content = Text.ToBinary(body, 1252)
        ]
    ),
    jsonResponse = Text.FromBinary(request)
in
    jsonResponse