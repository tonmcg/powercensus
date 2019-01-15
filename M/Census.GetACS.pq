let
/* Helper functions */
    combineValues = (columnName as text)=>
        let
            functionValue = {columnName,(r)=> if r is null then null else Text.Combine(List.Transform(r, Text.From), ";"), type text}
        in
            functionValue,

/* Parameters */
    vintage = Text.From("2015"),
    dataset = "acs5",
    columns = {"B01001_001E","B01001_002E"},
    geography = "urban area",

/* Variables */
    variables.url = "https://api.census.gov/data/" & vintage & "/" & dataset & "/variables.json",
    variables.response = Web.Contents(variables.url),
    variables.responseMetadata = Value.Metadata(variables.response),
    variables.responseCode = variables.responseMetadata[Response.Status],
    variables.responseHeaders = variables.responseMetadata[Headers],
    variables.json = Json.Document(variables.response),
    variables.record = variables.json[variables],
    variables.records = Record.ToTable(variables.record),
    variables.table = 
        let
            recordTable = Table.ExpandRecordColumn(variables.records, "Value", {"label", "concept", "predicateType", "group", "limit", "predicateOnly"},{"label", "concept", "Value", "group", "limit", "predicateOnly"}),
            filteredTable = Table.SelectRows(recordTable, (r)=> List.Contains(columns, r[Name])),
            mergedTitle = Table.CombineColumns(filteredTable,{"label", "concept"},Combiner.CombineTextByDelimiter(" ", QuoteStyle.None),"Title")
        in
            mergedTitle,

    // find variables
    variables.doExist = List.ContainsAll(variables.table[Name], columns),
    
    // get variable labels
    // create list of lists of variables/labels combinations
    variables.labels = 
        let
            SelectedColumns = Table.SelectColumns(variables.table,{"Name", "Title"}),
            fieldValues = Table.AddColumn(SelectedColumns, "Custom", each Record.FieldValues(_)),
            columnlabels = Table.RemoveColumns(fieldValues,{"Name", "Title"})[Custom]
        in
            columnlabels,
    // get variable data types
    // create list of lists of variables/data types combinations
    variables.dataTypes = 
        let
            SelectedColumns = Table.SelectColumns(variables.table,{"Name", "Value"}),
            ReplacedIntType = Table.ReplaceValue(SelectedColumns,"int","number",Replacer.ReplaceText,{"Value"}),
            ReplacedStringType = Table.ReplaceValue(ReplacedIntType,"string","text",Replacer.ReplaceText,{"Value"}),
            SetTypes = Table.TransformColumns(ReplacedStringType, {{"Value", each "type " & _}}),
            TextToType = Table.TransformColumns(SetTypes,{{"Value", Expression.Evaluate}}),
            fieldValues = Table.AddColumn(TextToType, "Custom", each Record.FieldValues(_)),
            columnTypes = Table.RemoveColumns(fieldValues,{"Name", "Value"})[Custom]
        in
            columnTypes,
    
/* Geographies */
    geographies.url = "https://api.census.gov/data/" & vintage & "/" & dataset & "/geography.json",
    geographies.response = Web.Contents(geographies.url),
    geographies.responseMetadata = Value.Metadata(geographies.response),
    geographies.responseCode = geographies.responseMetadata[Response.Status],
    geographies.responseHeaders = geographies.responseMetadata[Headers],
    geographies.json = Json.Document(geographies.response),
    geographies.list = geographies.json[fips],
    geographies.records = Table.FromList(geographies.list, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    geographies.table = Table.ExpandRecordColumn(geographies.records, "Column1", {"name", "geoLevelId", "referenceDate", "requires", "wildcard", "optionalWithWCFor"}),
    geographies.requires = Table.TransformColumns(geographies.table,combineValues("requires")),
    geographies.wildcard = Table.TransformColumns(geographies.requires,combineValues("wildcard")),
    
/* Response */
    data.url = "https://api.census.gov/data/" & vintage & "/" & dataset & "?get=" & Text.Combine(columns, ",") & "&for=" & geography,
    data.response = Web.Contents(data.url),
    data.responseMetadata = Value.Metadata(data.response),
    data.responseCode = data.responseMetadata[Response.Status],
    data.responseHeaders = data.responseMetadata[Headers],
    data.json = Json.Document(data.response),


/* Data */
    ConvertedToTable = Table.FromList(data.json, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    ParsedLists = Table.TransformColumns(ConvertedToTable, {"Column1", each Text.Combine(List.Transform(_, Text.From), ";"), type text}),
    SplitListValues = Table.SplitColumn(ParsedLists, "Column1", Splitter.SplitTextByDelimiter(";", QuoteStyle.Csv), List.Combine({columns,{"geography"}})),
    PromotedHeaders = Table.PromoteHeaders(SplitListValues, [PromoteAllScalars=true]),
    DefinedDataTypes = Table.TransformColumnTypes(PromotedHeaders,variables.dataTypes),
    AssignedColumnLabels = Table.RenameColumns(DefinedDataTypes,variables.labels)
in
    AssignedColumnLabels