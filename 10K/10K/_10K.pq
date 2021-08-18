// This file contains your Data Connector logic
section _10K;

// Data Source Kind
_10K = [
    Authentication = [
        Key = [
            KeyLabel = Extension.LoadString("AuthorizationToken"),
            Label = Extension.LoadString("AuthorizationLabel")
        ]
    ],
    Label = Extension.LoadString("DataSourceLabel")
];

// Data Source UI publishing description
_10K.Publish = [
    Beta = true,
    Category = "Online Services",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://www.10000ft.com/",
    SourceImage = _10K.Icons,
    SourceTypeImage = _10K.Icons
];

_10K.Icons = [
    Icon16 = { Extension.Contents("_10K16.png"), Extension.Contents("_10K20.png"), Extension.Contents("_10K24.png"), Extension.Contents("_10K32.png") },
    Icon32 = { Extension.Contents("_10K32.png"), Extension.Contents("_10K40.png"), Extension.Contents("_10K48.png"), Extension.Contents("_10K64.png") }
];


//
// Implementation
//

// URI Definitions
BaseUri =  "https://api.10000ft.com";
ApiPath = "/api/v1/";

// Schema Transforms
// Table Schema Definitions
SchemaTable = #table({"Entity", "SchemaTable"}, {
    {"projects", #table({"Name", "Type"}, {
        {"id", Int64.Type},
        {"name", type text},
        {"client", type text},
        {"project_state", type text},
        {"phase_name", type nullable text},
        {"archived", type logical},
        {"archived_at", type nullable datetime},
        {"description", type nullable text},
        {"parent_id", type nullable number},
        {"project_code", type nullable text},
        {"secureurl", type nullable text},
        {"secureurl_expiration", type nullable text},
        {"settings", type number},
        {"timeentry_lockout", type number},
        {"ends_at", type datetime},
        {"starts_at", type datetime},
        {"use_parent_bill_rates", type logical},
        {"thumbnail", type nullable text},
        {"type", type text},
        {"has_pending_updates", type logical},
        {"custom_field_values", type nullable record}
    })},
    {"users", #table({"Name", "Type"}, {
        {"id", Int64.Type},
        {"first_name", type text},
        {"last_name", type text},
        {"display_name", type nullable text},
        {"email", type text},
        {"user_type_id", type number},
        {"billable", type logical},
        {"hire_date", type nullable datetime},
        {"termination_date", type nullable datetime},
        {"mobile_phone", type nullable text},
        {"office_phone", type nullable text},
        {"archived", type logical},
        {"archived_at", type nullable datetime},
        {"deleted", type logical},
        {"deleted_at", type nullable datetime},
        {"account_owner", type nullable logical},
        {"invitation_pending", type nullable logical},
        {"user_settings", type nullable text},
        {"employee_number", type nullable text},
        {"role", type nullable text},
        {"discipline", type nullable text},
        {"location", type nullable text},
        {"type", type text},
        {"has_login", type logical},
        {"login_type", type text},
        {"license_type", type nullable text},
        {"thumbnail", type nullable text}
    })},
    {"time_entries", #table({"Name", "Type"}, {
        {"id", Int64.Type},
        {"user_id", type number},
        {"assignable_id", type number},
        {"assignable_type", type text},
        {"date", type datetime},
        {"hours", type number},
        {"is_suggestion", type nullable logical},
        {"scheduled_hours", type nullable number},
        {"task", type nullable text},
        {"notes", type nullable text},
        {"bill_rate", type nullable number},
        {"bill_rate_id", type nullable number}
    })}
});

PerPage = "1000";
EntityFilters = #table({"Entity", "Params", "HasDateFilter", "HasCustomFields"}, {
    {"projects", [fields = "custom_field_values", per_page = PerPage], false, true},
    {"users", [with_archived = "true", include_placeholders = "true", per_page = PerPage], false, false},
    {"time_entries", [with_suggestions = "true", per_page = PerPage], true, false}
});
GetSchemaForEntity = (entity as text) as table => try SchemaTable{[Entity=entity]}[SchemaTable] otherwise error "Couldn't find entity: '" & entity & "'";

[DataSource.Kind="_10K", Publish="_10K.Publish"]
shared _10K.Contents = (#"Start Date" as date, #"Number of Months" as number) => _10KNavTable(#"Start Date", #"Number of Months");// as table;

// common function to generate format the results into a nav table.
_10KNavTable = (startDate as date, numMonths as number) => //as table =>
    let
        url = BaseUri & ApiPath,
        sd = Date.StartOfMonth(startDate),
        ed = Date.EndOfMonth(Date.AddMonths(sd, numMonths - 1)),
        startDateStr = GetFormattedDateString(sd),
        endDateStr = GetFormattedDateString(ed),

        // Use our schema table as the source of top level items in the navigation tree
        entities = Table.SelectColumns(SchemaTable, {"Entity"}),
        rename = Table.RenameColumns(entities, {{"Entity", "Name"}}),
        withData = Table.AddColumn(
                                    rename,
                                    "Data",
                                    each GetEntity(
                                                    url,
                                                    [Name],
                                                    EntityFilters{[Entity = [Name]]},
                                                    startDateStr,
                                                    endDateStr
                                                  ),
                                    type table
                                  ),
        navTable = CreateNavTable(withData)
    in
        navTable{[Name = "projects"]}[Data]{[id = 1594539]}[custom_field_values];

CreateNavTable = (base as table) as table =>
    let
        // Add ItemKind and ItemName as fixed text values
        withItemKind = Table.AddColumn(base, "ItemKind", each "Table", type text),
        withItemName = Table.AddColumn(withItemKind, "ItemName", each "Table", type text),
        // Indicate that the node should not be expandable
        withIsLeaf = Table.AddColumn(withItemName, "IsLeaf", each true, type logical),
        // Generate the nav table
        navTable = Table.ToNavigationTable(withIsLeaf, {"Name"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")
    in
        navTable;


_10K.Feed = (url as text, optional schema as table) as table => 
    let
        apiKey = Extension.CurrentCredential()[Key],
        header = [
            #"Accept" = "application/json",                         // column name and values only
            #"auth" = apiKey                                        // Authorization Token
        ]
    in
        GetAllPagesByNextLink(url, header, schema);

GetProjects = (data as table) => TransponseCustomFields(Table.ExpandRecordColumn(data, "custom_field_values", {"data"}));

TransponseCustomFields = (customFields as table) =>
    let
        #"Expanded Custom Data" = Table.ExpandListColumn(customFields, "data"),
        #"Expanded Custom Data1" = Table.ExpandRecordColumn( #"Expanded Custom Data", "data", {"custom_field_name", "value"}, {"custom_field_name.1", "value.1"}),
        #"Pivoted Column" = Table.Pivot(#"Expanded Custom Data1", List.Distinct(#"Expanded Custom Data1"[custom_field_name.1]), "custom_field_name.1", "value.1")
    in
        #"Pivoted Column";


GetFormattedDateString = (d as date) as text => Date.ToText(d, "yyyy-MM-dd");

// Read all pages of data.
// After every page, we check the "NextLink" record on the metadata of the previous request.
// Table.GenerateByPage will keep asking for more pages until we return null.
GetAllPagesByNextLink = (url as text, header as record, optional schema as table) as table =>
    Table.GenerateByPage((previous) => 
        let
            // if previous is null, then this is our first page of data
            nextLink = if (previous = null) then url else BaseUri & Value.Metadata(previous)[NextLink]?,
            // if NextLink was set to null by the previous call, we know we have no more data
            page = if (nextLink <> null) then GetPage(header, nextLink, schema) else null
        in
            page
    );

GetPage = (header as record, url as text, optional schema as table) as table =>
    let
        response = Web.Contents(url, [ Headers = header, ManualCredentials = true ]),        
        body = Json.Document(response),
        nextLink = GetNextLink(body[paging]),
        data = Table.FromRecords(body[data]),
        withSchema = if (schema = null) then data else SchemaTransformTable(data, schema)
    in
        withSchema meta [NextLink = nextLink];

// In this implementation, 'response' will be the parsed body of the response after the call to Json.Document.
// Look for the 'next' field and simply return null if it doesn't exist.
GetNextLink = (response) as nullable text => Record.FieldOrDefault(response, "next");

// Entity Functions
GetEntity = (url as text, entity as text, entityFilters as record, startDate as text, endDate as text) as table => 
    let
        params = entityFilters[Params],
        hasDateFilter = entityFilters[HasDateFilter],
        hasCustomFields = entityFilters[HasCustomFields],
        pathUrl = Uri.Combine(url, entity),
        fullUrl = if (params = null) then pathUrl else
            let
                // Add date filtering if required
                fullParams = if hasDateFilter
                    then
                        let
                            p1 = Record.AddField(params, "from", startDate),
                            p2 = Record.AddField(p1, "to", endDate)
                        in
                            p2
                    else
                        params
            in
                pathUrl & "?" & Uri.BuildQueryString(params),
        schema = GetSchemaForEntity(entity),
        result = _10K.Feed(fullUrl, schema),
        withCustomFields = if hasCustomFields then TransponseCustomFields(Table.ExpandRecordColumn(result, "custom_field_values", {"data"})) else result

    in
        result;

EnforceSchema.Strict = 1;               // Add any missing columns, remove extra columns, set table type
EnforceSchema.IgnoreExtraColumns = 2;   // Add missing columns, do not remove extra columns
EnforceSchema.IgnoreMissingColumns = 3; // Do not add or remove columns

SchemaTransformTable = (table as table, schema as table, optional enforceSchema as number) as table =>
    let
        // Default to EnforceSchema.Strict
        _enforceSchema = if (enforceSchema <> null) then enforceSchema else EnforceSchema.Strict,

        // Applies type transforms to a given table
        EnforceTypes = (table as table, schema as table) as table =>
            let
                map = (t) => if Type.Is(t, type list) or Type.Is(t, type record) or t = type any then null else t,
                mapped = Table.TransformColumns(schema, {"Type", map}),
                omitted = Table.SelectRows(mapped, each [Type] <> null),
                existingColumns = Table.ColumnNames(table),
                removeMissing = Table.SelectRows(omitted, each List.Contains(existingColumns, [Name])),
                primativeTransforms = Table.ToRows(removeMissing),
                changedPrimatives = Table.TransformColumnTypes(table, primativeTransforms)
            in
                changedPrimatives,

        // Returns the table type for a given schema
        SchemaToTableType = (schema as table) as type =>
            let
                toList = List.Transform(schema[Type], (t) => [Type=t, Optional=false]),
                toRecord = Record.FromList(toList, schema[Name]),
                toType = Type.ForRecord(toRecord, false)
            in
                type table (toType),

        // Determine if we have extra/missing columns.
        // The enforceSchema parameter determines what we do about them.
        schemaNames = schema[Name],
        foundNames = Table.ColumnNames(table),
        addNames = List.RemoveItems(schemaNames, foundNames),
        extraNames = List.RemoveItems(foundNames, schemaNames),
        tmp = Text.NewGuid(),
        added = Table.AddColumn(table, tmp, each []),
        expanded = Table.ExpandRecordColumn(added, tmp, addNames),
        result = if List.IsEmpty(addNames) then table else expanded,
        fullList =
            if (_enforceSchema = EnforceSchema.Strict) then
                schemaNames
            else if (_enforceSchema = EnforceSchema.IgnoreMissingColumns) then
                foundNames
            else
                schemaNames & extraNames,

        // Select the final list of columns.
        // These will be ordered according to the schema table.
        reordered = Table.SelectColumns(result, fullList, MissingField.Ignore),
        enforcedTypes = EnforceTypes(reordered, schema),
        withType = if (_enforceSchema = EnforceSchema.Strict) then Value.ReplaceType(enforcedTypes, SchemaToTableType(schema)) else enforcedTypes
    in
        withType;

Extension.LoadFunction = (name as text) =>
    let
        binary = Extension.Contents(name),
        asText = Text.FromBinary(binary)
    in
        Expression.Evaluate(asText, #shared);

Table.ChangeType = Extension.LoadFunction("Table.ChangeType.pqm");
Table.GenerateByPage = Extension.LoadFunction("Table.GenerateByPage.pqm");
Table.ToNavigationTable = Extension.LoadFunction("Table.ToNavigationTable.pqm");
