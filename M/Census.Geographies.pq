let
	geographies = 
		{
			Record.FromList(
				{"State","state","2","county","STATE",null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"Urban Area","ua","2",null,null,null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"Core Based Statistical Area","cbsa","3","cbsa","GEOID",null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"New England City and Town Area","necta","2","necta","NECTA",null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"County","counties","4","county","GEOID",null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"County Subdivision","cousubs","6","cousub","GEOID",null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"Place","place","5","place","GEOID",null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"Census Tract","tracts","5","tract","GEOID",null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"Congressional District","CDs","8",null,null,
					{
						Record.FromList({2012, "112"}, type [year = number, meeting = text]),
						Record.FromList({2013, "113"}, type [year = number, meeting = text]),
						Record.FromList({2014, "114"}, type [year = number, meeting = text]),
						Record.FromList({2015, "114"}, type [year = number, meeting = text]),
						Record.FromList({2016, "115"}, type [year = number, meeting = text]),
						Record.FromList({2017, "115"}, type [year = number, meeting = text]),
						Record.FromList({2018, "116"}, type [year = number, meeting = text])
					}
				}, 
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"School Districts - Elementary","elsd","11","Elemnetary School Districts",null,null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"School Districts - Secondary","scsd","11","Secondary School Districts",null,null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"School Districts - Unified","unsd","11","Unified School Districts",null,null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"ZIP Code Tabulation Area","zcta","1","ZIP Code Tabulation Area",null,null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"State Legislative Districts - Lower Chamber","sldl","9","Lower State Legislative Districts",null,null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"State Legislative Districts - Upper Chamber","sldu","9","Upper State Legislative Districts",null,null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"American Indian/Alaska Native/Native Hawaiian Areas","aiannh","9","Indigenous Areas",null,null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"American Indian/Alaska Native/Native Hawaiian Areas - Reservations Only","aiannhr","9","Indigenous Reservations",null,null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			),
			Record.FromList(
				{"American Indian/Alaska Native/Native Hawaiian Areas - Trust Lands Only","aiannht","9","Indigenous Trusts",null,null},
				type [geography = text, gazetteerUrlPart = text, numOfColumns = number, zipUrlPart = text, zipId = text, yearList = list]
			)
		}
in
    geographies