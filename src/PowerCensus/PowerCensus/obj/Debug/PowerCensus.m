section Census;

[DataSource.Kind="Census", Publish="Census.Publish"]
shared Census.ListApis = Value.ReplaceType(CensusCall, type function () as any);

CensusCall = () =>
let
    Source = Json.Document(Web.Contents("https://api.census.gov/data.json"))[dataset],
    ConvertedToTable = Table.FromList(Source, Splitter.SplitByNothing(), {"dataset"}, null, ExtraValues.Error),
    ExpandedDataset = Table.ExpandRecordColumn(ConvertedToTable, "dataset", {"title", "c_dataset", "c_vintage", "distribution", "c_isTimeseries", "temporal", "description", "modified"}, {"title", "name", "vintage", "url", "isTimeseries", "temporal", "description", "modified"}),
    ExtractedNames = Table.TransformColumns(ExpandedDataset, {"name", each Text.Combine(List.Transform(_, Text.From), "/"), type text}),
    ExpandedDistributionObject = Table.ExpandListColumn(ExtractedNames, "url"),
    ExpandedURL = Table.ExpandRecordColumn(ExpandedDistributionObject, "url", {"accessURL"}, {"url"}),
    DefinedDataTypes = Table.TransformColumnTypes(ExpandedURL,{{"title", type text}, {"vintage", Int64.Type}, {"url", type text}, {"isTimeseries", type logical}, {"temporal", type text}, {"description", type text}, {"modified", type datetime}}),
    SortedRows = Table.Sort(DefinedDataTypes,{{"name", Order.Ascending}, {"vintage", Order.Ascending}})
in
    SortedRows;

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
