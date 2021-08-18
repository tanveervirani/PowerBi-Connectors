// Use this file to write queries to test your data connector
let
	source = _10K.Contents(Date.From(DateTime.LocalNow()), 3),
	//data = source{[Name = "projects"]}[Data]//{[id = 1594539]}[custom_field_values][data]{0}
	data = source
in
	data
/*
let
	source = _10K.Contents(),
	data = source{[Name="TimeEntries"]}[Data]
in
	data
*/