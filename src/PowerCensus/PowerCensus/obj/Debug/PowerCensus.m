section Census;

[DataSource.Kind="Census", Publish="Census.Publish"]
shared Census.ListApis = Value.ReplaceType(CensusCall, type function () as any);

CensusCall = () =>
let
    Source = Json.Document(Web.Contents("https://api.census.gov/data.json")),
    dataset = Source[dataset],
    ConvertedToTable = Table.FromList(dataset, Splitter.SplitByNothing(), {"dataset"}, null, ExtraValues.Error),
    ExpandedDataset = Table.ExpandRecordColumn(ConvertedToTable, "dataset", {"c_vintage", "c_dataset", "c_geographyLink", "c_variablesLink", "c_tagsLink", "c_examplesLink", "c_groupsLink", "c_valuesLink", "c_documentationLink", "c_isAggregate", "c_isAvailable", "@type", "title", "accessLevel", "bureauCode", "description", "distribution", "contactPoint", "identifier", "keyword", "license", "modified", "programCode", "references", "spatial", "temporal", "publisher", "c_isCube", "c_isTimeseries"}, {"c_vintage", "c_dataset", "c_geographyLink", "c_variablesLink", "c_tagsLink", "c_examplesLink", "c_groupsLink", "c_valuesLink", "c_documentationLink", "c_isAggregate", "c_isAvailable", "@type", "title", "accessLevel", "bureauCode", "description", "distribution", "contactPoint", "identifier", "keyword", "license", "modified", "programCode", "references", "spatial", "temporal", "publisher", "c_isCube", "c_isTimeseries"}),
    ExtractedDatasets = Table.TransformColumns(ExpandedDataset, {"c_dataset", each Text.Combine(List.Transform(_, Text.From), ";"), type text}),
    ExtractedBureauCode = Table.TransformColumns(ExtractedDatasets, {"bureauCode", each Text.Combine(List.Transform(_, Text.From), ";"), type text}),
    ExpandedDistributionList = Table.ExpandListColumn(ExtractedBureauCode, "distribution"),
    ExpandedDistribution = Table.ExpandRecordColumn(ExpandedDistributionList, "distribution", {"@type", "accessURL", "description", "format", "mediaType", "title"}, {"distribution.@type", "id", "distribution.description", "distribution.format", "distribution.mediaType", "distribution.title"}),
    ExpandedContacts = Table.ExpandRecordColumn(ExpandedDistribution, "contactPoint", {"fn", "hasEmail"}, {"contactPoint.fn", "contactPoint.hasEmail"}),
    ExtractedKeywords = Table.TransformColumns(ExpandedContacts, {"keyword", each Text.Combine(List.Transform(_, Text.From), ";"), type text}),
    ExtractedProgramCode = Table.TransformColumns(ExtractedKeywords, {"programCode", each Text.Combine(List.Transform(_, Text.From), ";"), type text}),
    ExtractedRefereneces = Table.TransformColumns(ExtractedProgramCode, {"references", each Text.Combine(List.Transform(_, Text.From), ";"), type text}),
    ExpandedPublisher = Table.ExpandRecordColumn(ExtractedRefereneces, "publisher", {"name"}, {"publisher.name"}),
    ReplacedErrors = Table.ReplaceErrorValues(ExpandedPublisher, {{"bureauCode", null}, {"keyword", null}, {"programCode", null}, {"references", null}}),
    DefinedDataTypes = Table.TransformColumnTypes(ReplacedErrors,{{"c_vintage", Int64.Type}, {"c_dataset", type text}, {"c_geographyLink", type text}, {"c_variablesLink", type text}, {"c_tagsLink", type text}, {"c_examplesLink", type text}, {"c_groupsLink", type text}, {"c_valuesLink", type text}, {"c_documentationLink", type text}, {"c_isAggregate", type logical}, {"c_isAvailable", type logical}, {"@type", type text}, {"title", type text}, {"accessLevel", type text}, {"bureauCode", type text}, {"description", type text}, {"id", type text}, {"contactPoint.fn", type text}, {"contactPoint.hasEmail", type text}, {"identifier", type text}, {"keyword", type text}, {"license", type text}, {"modified", type datetime}, {"programCode", type text}, {"references", type text}, {"spatial", type text}, {"temporal", type text}, {"publisher.name", type text}, {"c_isCube", type logical}, {"c_isTimeseries", type logical}, {"distribution.@type", type text}, {"distribution.description", type text}, {"distribution.format", type text}, {"distribution.mediaType", type text}, {"distribution.title", type text}}),
    AddedPrimaryKey = Table.AddKey(DefinedDataTypes, {"id"}, true)
in
    AddedPrimaryKey;

BaseUri = "https://api.census.gov/data.json";

// Returns the contents of all table nodes within the HTML document broken into its constituent structures
GetTables = (url as text) =>
    let
        DOM = Text.FromBinary(Web.Contents(url)),
        DOCTYPE = 
            let
                DOCTag = "<!" & Text.BetweenDelimiters(DOM, "<!", ">") & ">"
            in
                DOCTag,
        HTMLOpeningTag = 
            let
                HtmlTag = "<html" & Text.BetweenDelimiters(DOM, "<html", ">") & ">"
            in
                HtmlTag,
        HEAD = 
            let
                HeadString = "<head" & Text.BetweenDelimiters(DOM, "<head", "</head>") & "</head>"
            in
                HeadString,
        BODYOpeningTag = 
            let
                BodyString = "<body>"
            in
                BodyString,
        GetTables = (n as number) =>
    	    let
	            CurrentTable = Text.BetweenDelimiters(DOM, "<table", "</table>", n)
            in
                if CurrentTable = "" then 
                    ""
                else
                    Text.Combine({ "<table", CurrentTable, "</table>", @GetTables(n+1) }),
	    TABLES = GetTables(0),
        HTML = Text.Combine({DOCTYPE, HTMLOpeningTag, HEAD, BODYOpeningTag, TABLES, "</body></html>"}),
        Page = Web.Contents(HTML),
        Tables = Table.SelectRows(Page, each ([Source] = "Table"))
    in
        Tables;

// Get current list of U.S. States and Outlying Areas
// Source: https://www.census.gov/geo/reference/ansi_statetables.html
Census.GetStates = (optional includeTerritories as logical) as table =>
    let
        StateTable = GetTables("https://www.census.gov/geo/reference/ansi_statetables.html"),
        States = 
            let
                Data = StateTable{0}[Data],
                SelectedGazetteerColumns = Table.SelectColumns(Data,{"Name", "FIPS State Numeric Code", "Official USPS Code"}),
                RenamedGazetteerColumns = Table.RenameColumns(SelectedGazetteerColumns,{{"Name", "state_name"}, {"FIPS State Numeric Code", "state_fips"}, {"Official USPS Code", "state_abbr"}})
            in
                RenamedGazetteerColumns,
        Territories = 
            let
                Data = StateTable{1}[Data],
                SelectedGazetteerColumns = Table.SelectColumns(Data,{"Area Name", "FIPS State Numeric Code", "Official USPS Code"}),
                RenamedGazetteerColumns = Table.RenameColumns(SelectedGazetteerColumns,{{"Area Name", "state_name"}, {"FIPS State Numeric Code", "state_fips"}, {"Official USPS Code", "state_abbr"}})
            in
                RenamedGazetteerColumns,
        UnionStates = if not includeTerritories or includeTerritories = null then States else Table.Combine({States, Territories})
in
    UnionStates;

// Data Source Kind description
Census = [
    Authentication = [
        Anonymous = []
    ],
    Label = "Census API"
];

// Data Source UI publishing description
Census.Publish = [
    Beta = true,
    Category = "Other",
    ButtonText = { "Census API", "Census API" }
];

Census.Icons = [
    Icon16 = { Extension.Contents("PowerCensus16.png"), Extension.Contents("PowerCensus20.png"), Extension.Contents("PowerCensus24.png"), Extension.Contents("PowerCensus32.png") },
    Icon32 = { Extension.Contents("PowerCensus32.png"), Extension.Contents("PowerCensus40.png"), Extension.Contents("PowerCensus48.png"), Extension.Contents("PowerCensus64.png") }
];
