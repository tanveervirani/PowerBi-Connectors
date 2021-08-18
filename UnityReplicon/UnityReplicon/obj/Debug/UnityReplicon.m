// This file contains your Data Connector logic
section UnityReplicon;

gss_url = try Text.FromBinary(Web.Contents("http://127.0.0.1:7613")) otherwise "https://global.replicon.com";
client_id = Text.FromBinary(Extension.Contents("client_id"));
client_secret = Text.FromBinary(Extension.Contents("client_secret"));
redirect_uri = "https://oauth.powerbi.com/views/oauthredirect.html";
windowWidth = 800;
windowHeight = 700;

[DataSource.Kind="UnityReplicon", Publish="UnityReplicon.Publish"]
/*
shared UnityReplicon.Contents = (#"Start Date" as date, optional #"End Date" as date) =>
    let
        sd = if #"Start Date" = null then Date.From(Date.StartOfYear(DateTime.LocalNow())) else #"Start Date",
        dateRange = [ Start = sd, End = #"End Date"]
    in
        Value.ReplaceType(() => ContentsImpl(dateRange), ContentsType)();
*/
shared UnityReplicon.Contents1 = () =>
    let
        sd =  Date.From(Date.StartOfYear(DateTime.LocalNow())),
        ed =  Date.From(Date.EndOfYear(DateTime.LocalNow())),
        dateRange = [ Start = sd, End = ed ]
    in
        Value.ReplaceType(() => ContentsImpl(dateRange), ContentsType)();

ContentsType = type function ()
    as table meta [
        Documentation.Name = "Contents",
        Documentation.LongDescription = "Retrieves all the tables which are available to be extracted from Replicon.  This is the main navigation table for the connector.",
        Documentation.Examples = {[
            Description = "Returns a table containing the data sets available for extraction",
            Code = "UnityReplicon.Contents()",
            Result = "#table({""Table Id"", ""Table Name"", ""Table Type"", ""Table Data"", ""ItemKind"", ""ItemName"", ""IsLeaf""}, {{""BillingRate"", ""Billing Rate"", <Column List>, <Data Load Func>, ""Table"", ""Table"", TRUE}})"
        ]}
    ];

ContentsImpl = (dateRange as record) =>
    let
        t0 = GetAllTables(),
        t1 = Table.AddColumn(t0, "Table Data", each LoadData([#"Table Id"], [#"Table Type"], [#"Support DateRange Filtering"], dateRange)),
        t2 = Table.AddColumn(t1, "ItemKind", each "Table"),
        t3 = Table.AddColumn(t2, "ItemName", each "Table"),
        t4 = Table.AddColumn(t3, "IsLeaf", each true)
    in
        Table.ToNavigationTable(t4, {"Table Id"}, "Table Name", "Table Data", "ItemKind", "ItemName", "IsLeaf");

// Data Source Kind description
UnityReplicon = [
    TestConnection = (dataSourcePath) => { "UnityReplicon.Contents" },
    Authentication = [
        OAuth = [
            StartLogin=StartLogin,
            FinishLogin=FinishLogin,
            Refresh=Refresh,
            Label=Extension.LoadString("AccountLabel")
        ]
    ],
    Label = Extension.LoadString("DataSourceLabel"),
    Icons = UnityReplicon.Icons
];

// Data Source UI publishing description
UnityReplicon.Publish = [
    Beta = true,
    Category = "Online Services",
    ButtonText = { Extension.LoadString("ExtensionTitle"), Extension.LoadString("ExtensionDescription") },
    LearnMoreUrl = "https://www.replicon.com/",
    SourceImage = UnityReplicon.Icons,
    SourceTypeImage = UnityReplicon.Icons
];

UnityReplicon.Icons = [
    Icon16 = { Extension.Contents("UnityReplicon16.png"), Extension.Contents("UnityReplicon20.png"), Extension.Contents("UnityReplicon24.png"), Extension.Contents("UnityReplicon32.png") },
    Icon32 = { Extension.Contents("UnityReplicon32.png"), Extension.Contents("UnityReplicon40.png"), Extension.Contents("UnityReplicon48.png"), Extension.Contents("UnityReplicon64.png") }
];

headers = [
    #"Content-Type" = "application/json",
    #"Accept" = "application/json"
];

url = (suffix as text) =>
    Extension.CurrentCredential()[Properties][dwrUrl] & "/analytics/" & suffix;

LoadData = (tableId as text, tableType as list, supportDateRangeFiltering as logical, dateRange as record) as table =>
    let
        extractId = StartExtract({ [ id = tableId, doDateRangeFiltering = supportDateRangeFiltering, dateRangeParam = dateRange] }),
        completedExtract = WaitForCompletion(extractId),
        dataUrls = completedExtract[dataUrls],
        untypedTableData = DownloadCSV(Record.Field(dataUrls, tableId))
    in
        Table.TransformColumnTypes(untypedTableData, tableType, "en-US");

WaitForCompletion = (extractId as text) as record =>
    // I bet you're wondering why this is so needlessly complex...well, let me tell you. Microsoft, in their infinite wisdom, implemented
    // automatic oauth credential refresh in PowerBI Desktop, but when the connector is running from PowerBI Online (via the On-Premises Data
    // Gateway) it will just happily let the access token expire and not even attempt to refresh it...so here, begrudgingly, we do it ourselves.
    let
        ret = Value.WaitForStateful(() => [
                AccessToken = Extension.CurrentCredential()[access_token]
            ],
            (iteration, state) => 
                let
                    extractStatus = GetExtractStatus(extractId, state[AccessToken]),
                    newState = if extractStatus = null then 
                                   [AccessToken = Refresh(null, Extension.CurrentCredential()[refresh_token])[access_token] ]
                               else 
                                   state,
                    val = if extractStatus = null then null else if Record.FieldOrDefault(extractStatus, "status") = "completed" then extractStatus else null
                in
                    { val, newState },
            (iteration, state) => #duration(0, 0, 0, 1)
        )
    in 
        ret;

GetExtractStatus = (extractId as text, accessToken as text) as nullable record =>
    let 
        resp = Web.Contents(url("extracts/" & extractId), [ 
            Headers =  Record.Combine({
                headers,
                [ #"Authorization" = "Bearer " & accessToken ]
            }), 
            IsRetry = true, 
            ManualCredentials = true,
            ManualStatusHandling = {401}
        ]),
        doc = if Value.Metadata(resp)[Response.Status] = 401 then null else Json.Document(resp)
    in 
        doc;

GetStartExtractParam = (tables as list) =>
    let
        tablesJson = List.Transform(tables, each GetTableJson([id], [doDateRangeFiltering], GetDateRangeFilteringParam([dateRangeParam])) )
    in
        [
            description = "asd",
            visible = true,
            target = [
                #"type" = "download",
                format = "csv"
            ],
            tables = tablesJson
        ];

GetDateRangeFilteringParam = (dateRangeParam as record) =>
    let
        minDate = "2000-01-01",
        maxDate = "2099-12-31",

        /***************
         * Dates passed are all absolute, not relative, so rework following replicon logic
         * =========================================================================
        rangeStartDate = if dateRangeParam[Start] is null then minDate else DateTime.ToText(Date.AddDays(DateTime.LocalNow(), dateRangeParam[Start]), "yyyy-MM-dd"),
        rangeEndDate = if dateRangeParam[End] is null then maxDate else DateTime.ToText(Date.AddDays(DateTime.LocalNow(), dateRangeParam[End]), "yyyy-MM-dd")
         * =========================================================================
         */
        rangeStartDate = if dateRangeParam[Start] is null then minDate else Date.ToText(dateRangeParam[Start], "yyyy-MM-dd"),
        rangeEndDate = if dateRangeParam[End] is null then maxDate else Date.ToText(dateRangeParam[End], "yyyy-MM-dd")
    in
        [ startDate = rangeStartDate, endDate = rangeEndDate ];

GetTableJson = (tableId as text, doDateRangeFiltering as logical, dateRangeFilteringParam as record) =>
    if doDateRangeFiltering then
        [
            tableId = tableId,
            filters = [
                dateRange = dateRangeFilteringParam
            ]
        ]
    else
        [
            tableId = tableId
        ];

StartExtract = (tables as list) as text =>
    let
        resp = Json.Document(Web.Contents(url("extracts"), [ Headers =  headers, Content = Json.FromValue(GetStartExtractParam(tables)) ]))
    in
        resp[extractId];

GetAllTables = () =>
    let
        json = Json.Document(Web.Contents(url("tables"), [ Headers =  headers ]))
    in
        Table.FromRecords(List.Transform(json, each [ #"Table Id" = [id], #"Table Name" = [id], #"Table Type" = GetTableType([columns]), #"Support DateRange Filtering" = GetSupportDateRangeFiltering([filters]) ]));

GetSupportDateRangeFiltering = (filters as list) as logical =>
    List.Contains(filters, "date-range");

GetTableType = (columns as list) as list =>
    List.Transform(columns, each { [id], MapAATypeToMType([#"type"]) });

MapAATypeToMType = (aaType as text) as type =>
    let
        map = [
            string = type nullable text,
            int = type nullable Int64.Type,
            decimal = type nullable number,
            date = type nullable date,
            time = type nullable time,
            datetime = type nullable datetimezone,
            boolean = type nullable logical
        ]
    in
        Record.Field(map, aaType);

DownloadCSV = (url as text) =>
    let
        csv = Web.Contents(url, [ ManualCredentials = true ])
    in
        Table.PromoteHeaders(Csv.Document(csv));



// from https://docs.microsoft.com/en-us/power-query/helperfunctions
Value.WaitFor = (producer as function, interval as function, optional count as number) as any =>
    let
        list = List.Generate(
            () => {0, null},
            (state) => state{0} <> null and (count = null or state{0} < count),
            (state) => if state{1} <> null then {null, state{1}} else {1 + state{0}, Function.InvokeAfter(() => producer(state{0}), interval(state{0}))},
            (state) => state{1})
    in
        List.Last(list);

Value.WaitForStateful = (initialState as function, producer as function, interval as function, optional count as number) as any =>
    let
        list = List.Generate(
            () => {0, {null, initialState()}},
            (state) => state{0} <> null and (count = null or state{0} < count),
            (state) => if state{1}{0} <> null then {null, state{1}} else {1 + state{0}, Function.InvokeAfter(() => producer(state{0}, state{1}{1}), interval(state{0}, state{1}{1}))},
            (state) => state{1}{0})
    in
        List.Last(list);

// from https://docs.microsoft.com/en-us/power-query/helperfunctions#tabletonavigationtable
Table.ToNavigationTable = (
    table as table,
    keyColumns as list,
    nameColumn as text,
    dataColumn as text,
    itemKindColumn as text,
    itemNameColumn as text,
    isLeafColumn as text
) as table =>
    let
        tableType = Value.Type(table),
        newTableType = Type.AddTableKey(tableType, keyColumns, true) meta
        [
            NavigationTable.NameColumn = nameColumn,
            NavigationTable.DataColumn = dataColumn,
            NavigationTable.ItemKindColumn = itemKindColumn,
            Preview.DelayColumn = itemNameColumn,
            NavigationTable.IsLeafColumn = isLeafColumn
        ],
        navigationTable = Value.ReplaceType(table, newTableType)
    in
        navigationTable;

StartLogin = (resourceUrl, state, display) =>
    let
        authorizeUrl = gss_url & "/!/oauth2/authorize" & "?" & Uri.BuildQueryString([
            client_id = client_id,
            redirect_uri = redirect_uri,
            state = state,
            response_type = "code"
        ])
    in
        [
            LoginUri = authorizeUrl,
            CallbackUri = redirect_uri,
            WindowHeight = windowHeight,
            WindowWidth = windowWidth,
            Context = null
        ];

FinishLogin = (context, callbackUri, state) =>
    let
        parts = Uri.Parts(callbackUri)[Query],
        result = if (Record.HasFields(parts, {"error"})) then
                    error Error.Record(parts[error], parts[error_description], parts)
                 else
                    TokenMethod("authorization_code", "code", parts[code])
    in
        result;

Refresh = (resourceUrl, refresh_token) => TokenMethod("refresh_token", "refresh_token", refresh_token);

TokenMethod = (grantType, tokenField, code) =>
    let
        queryString = [
            grant_type = grantType,
            redirect_uri = redirect_uri
        ],
        queryWithCode = Record.AddField(queryString, tokenField, code),

        tokenResponse = Web.Contents(gss_url & "/!/oauth2/token", [
            Content = Text.ToBinary(Uri.BuildQueryString(queryWithCode)),
            Headers = [
                #"Content-type" = "application/x-www-form-urlencoded",
                #"Accept" = "application/json",
                #"Authorization" = "Basic " & Binary.ToText(Text.ToBinary(client_id & ":" & client_secret, TextEncoding.Utf8), BinaryEncoding.Base64)
            ],
            ManualStatusHandling = {400},
            IsRetry = true, 
            ManualCredentials = true
        ]),
        body = Json.Document(tokenResponse),
        result = if (Record.HasFields(body, {"error"})) then
                    error Error.Record(body[error], body[error_description], body)
                 else
                    Record.AddField(body, "dwrUrl", GetTenantEndpoint(body[access_token]))
    in
        result;

GetTenantEndpoint = (accessToken as text) =>
    let
        endpointResponse = Web.Contents(gss_url & "/DiscoveryService1.svc/GetTenantEndpointDetailsForAccessToken", [
            Content = Json.FromValue([ accessToken = accessToken ]),
            Headers = headers,
            ManualCredentials = true
        ]),
        body = Json.Document(endpointResponse)
    in
        body[d][applicationRootUrl];

