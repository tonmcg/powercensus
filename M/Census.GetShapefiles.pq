let
    Census.GetShapefiles = (state_fips as text, geography as text) =>
        let 
            shapefiles = Web.Contents("http://www2.census.gov/geo/tiger/GENZ2017/shp/cb_2017_" & state_fips & "_" & geography & "_500k.zip")
        in
            shapefiles
in
    Census.GetShapefiles