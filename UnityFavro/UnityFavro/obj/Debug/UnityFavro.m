// This file contains your Data Connector logic
section UnityFavro;

// URL Definitions
BaseUrl =  "https://favro.com/api/v1/";

// Data Source Kinds
UnityFavro = [
        Authentication = [
            UsernamePassword = [
                UsernameLabel = Extension.LoadString("UserNameLabel"),
                PasswordLabel = Extension.LoadString("PasswordLabel"),
                Label = Extension.LoadString("UserNamePasswordLabel")
            ]
        ],
    Label = Extension.LoadString("DataSourceLabel")
];

// Publish
UnityFavro.Publish = [
    Beta = true,
    Category = "Online Services",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://favro.com/",
    SourceImage = UnityFavro.Icons,
    SourceTypeImage = UnityFavro.Icons
];

// Icons
UnityFavro.Icons = [
    Icon16 = { Extension.Contents("UnityFavro16.png"), Extension.Contents("UnityFavro20.png"), Extension.Contents("UnityFavro24.png"), Extension.Contents("UnityFavro32.png") },
    Icon32 = { Extension.Contents("UnityFavro32.png"), Extension.Contents("UnityFavro40.png"), Extension.Contents("UnityFavro48.png"), Extension.Contents("UnityFavro64.png") }
];

//
// Implementation
//

DefaultHeaders = [
    #"Accept" = "application/json"                         // column name and values only
];

// Define top level table types
CardsType = type table [
    cardId = text,
    organizationId = text,
    widgetCommonId = text,
    todoListUserId = nullable text,
    todoListCompleted = nullable logical,
    columnId = nullable text,
    laneId = nullable text,
    parentCardId = nullable text,
    isLane = logical,
    archived = logical,
    listPosition = number,
    sheetPosition = number,
    cardCommonId = text,
    name = text,
    detailedDescription = text, 
    tags = {text},
    sequentialId = number,
    startDate = nullable date,
    dueDate = nullable date,
    assignments = {AssignmentType},
    numComments = number,
    tasksTotal = number,
    tasksDone = number,
    attachments = {text},
    customFields = {CustomItemFieldType},
    timeOnBoard = TimeOnBoardType,
    timeOnColumns = TimeOnColumnsType,
    favroAttachments = {FavroAttachmentsType}
];

CollectionsType = type table [
    collectionId = text,
    organizationId = text,
    name = text,
    sharedToUsers = {nullable OrganizationUserRoleType},
    publicSharing = text,
    background = text,
    archived = logical,
    fullMembersCanAddWidgets = logical
];

CustomFieldsType = type table [
    customFieldId = text,
    name = text,
    organiztionId = text,
    enabled = logical,
    #"type" = text,
    customFieldItems = nullable {nullable CustomFieldItemType}
];

OrganizationsType = type table [
    organizationid = text,
    name = text,
    sharedToUsers = {nullable OrganizationUserRoleType}
];

TagsType = type table [
    tagId = text,
    organizationId = text,
    name = text,
    color = text
];

UsersType = type table [
    userId = text,
    name = text,
    email = text,
    organizationRole = text
];

WidgetType = type table [
    widgetCommonId = text,
    organizationId = text,
    name = text,
    #"type" = text,
    color = text,
    ownerRole = text,
    editRole = text,
    collectionIds = {text},
    archived = logical
];

// Remaining Structure Types
AssignmentType = type [
    userId = text,
    completed = logical
];

CustomFieldItemType = type [
    customFieldItemId = text,
    name = text
];

CustomItemFieldType = type [
    customFieldId = text,
    // Need to figure this out.  Value field can be text, number or an array
    value = nullable any,
    total = nullable number,
    reports = nullable ReportType,
    timeline = nullable TimeLineType,
    link = LinkType
];

FavroAttachmentsType = type [
    itemCommonId = text,
    #"type" = text
];

LinkType = type [
    url = text,
    #"text" = text
];

OrganizationUserRoleType = type [
    userId = text,
    role = text,
    joinDate = datetime
];

ReportType = type [
    text = UserReportType
];

TimeLineType = type [
    startDate = datetime,
    dueDate = datetime,
    showTime = logical
];

TimeOnBoardType = type [
    time = number,
    isStopped = logical
];

TimeOnColumnsType = type [
    text = text
];

UserReportType = type [
    reportId = text,
    value = number,
    description = text,
    createdAt = datetime
];

SchemaTable = #table({"Entity", "Type"}, {
    {"cards", CardsType},
    {"collections", CollectionsType},
    {"customfields", CustomFieldsType},
    {"organizations", OrganizationsType},
    {"tags", TagsType},
    {"users", UsersType},
    {"widgets", WidgetType}
});

RequestTables = #table({"Name", "Type"}, {
    {"widgets", WidgetType}
});
GetSchemaForEntity = (entity as text) as nullable type => try SchemaTable{[Entity=entity]}[Type] otherwise null;

[DataSource.Kind="UnityFavro", Publish="UnityFavro.Publish"]

shared UnityFavro.Contents = (optional #"Collection Name" as text) =>
    let
        collectionName = if (#"Collection Name" = null) then "sol-vert-aeco" else #"Collection Name",
        organizationId = GetOrganizationId(),
        collectionId = GetCollectionId(collectionName, organizationId),
        customFieldsTable = GetCustomFields(organizationId),
        tags = GetTags(organizationId),
        users = GetUsers(organizationId),

        widgets = GetWidgets(organizationId, collectionId),
        cards = GetCards(organizationId, collectionId, widgets, customFieldsTable, tags, users),

        // Nav Table
        // IsLeaf indicates if the node is expandable or not in the UI
        baseNavTable = #table({"Name", "Data", "ItemKind", "ItemName", "IsLeaf"}, {
            {"widgets", widgets, "Table", "Table", true}, 
            {"cards", cards, "Table", "Table", false}
        }),

        // Generate the nav table
        navTable = Table.ToNavigationTable(baseNavTable, {"Name"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")
    in
        navTable{[Name = "cards"]}[Data]{[widgetCommonId = "12959323ae1bdef4ef7c83c9"]}[Card]{[cardId="0439e48c889fd80842e092ea"]}[tags];

// Gets the data for the entity
UnityFavro.Feed = (url as text, organizationId as text, optional schema as type) as table => GetAllPagesByNextLink(url, GetHeader(organizationId), schema);

// Get the Organization Id that we belong to.
// Required for all calls as part of the header
GetCards = (organizationId as text, collectionId as text, widgets as table, customFields as table, tags as table, users as table) as table =>
    let
        // Expanded Nav Table
        // IsLeaf indicates if the node is expandable or not in the UI
        cardsTable = #table({"widgetCommonId", "Name", "Card", "ItemKind", "ItemName", "IsLeaf"}, {
        }),
        widgetCommonIds = Table.Column(widgets, "widgetCommonId"),
        cards = List.Accumulate(widgetCommonIds, cardsTable, (table, widgetCommonId) =>
            Table.InsertRows(table, Table.RowCount(table),
                {[
                    widgetCommonId = widgetCommonId,
                    Name = Table.First(Table.SelectRows(widgets, each [widgetCommonId] = widgetCommonId), [name=""])[name],
                    Card = GetCard(organizationId, collectionId, Text.From(widgetCommonId), customFields, tags, users),
                    ItemKind = "Table",
                    ItemName = "Table",
                    IsLeaf = true
                ]})),
        cardNavTable = Table.ToNavigationTable(cards, {"widgetCommonId"}, "Name", "Card", "ItemKind", "ItemName", "IsLeaf")
    in
        cardNavTable;

// Retrieves a singe card, and transposes teh custom fields so that they are redered as part of the row for the card.
GetCard = (organizationId as text, collectionId as text, widgetCommonId as text, customFields as table, tags as table, users as table) as table =>
    let
        // Do not retrieve archived cards, as schem may have changed and  certain fields may not be on the cards.
        cards = GetEntity("cards", organizationId, [collectionId = collectionId, widgetCommonId = widgetCommonId, archived = "false"]),
        cardWithCustomTable = Table.AddColumn(cards, "___CustomFieldsRecords", each ProcessCard([customFields], customFields, tags, users), type record),
        cardwithExpandedRecordColumn = Table.ExpandRecordColumn(cardWithCustomTable, "___CustomFieldsRecords", {"custom_field_values"}),
        newCards = TransponseCustomFields(cardwithExpandedRecordColumn, "custom_field_values")
    in
        newCards;

TransponseCustomFields = (customFields as table, columnName as text) as table =>
    let
        expandedCustomData = Table.ExpandListColumn(customFields, columnName),
        expandedRecordColumn = Table.ExpandRecordColumn(expandedCustomData, columnName, {"name", "value"}, {"name.1", "value.1"}),
        pivotedColumn = Table.Pivot(expandedRecordColumn, List.Distinct(expandedRecordColumn[name.1]), "name.1", "value.1")
    in
        pivotedColumn;

ProcessCard = (customFieldIdList as list, customFields as table, tags as table, users as table) as record =>
    let
        customFieldsRecords = List.Accumulate(customFieldIdList, [], (customFieldList, customFieldRow) => ProcessCardCustomItemsAndAddToRecord(customFieldList, customFieldRow, customFields, tags, users)),
        recordFieldNameList = Record.FieldNames(customFieldsRecords),
        recordList = List.Accumulate(recordFieldNameList, {}, (state, current) => List.Combine({state, {[name = current, value = Record.Field(customFieldsRecords, current)]}})),
        record =  [custom_field_values = recordList]
    in
       record;

ProcessCardCustomItemsAndAddToRecord = (state as record, current as record, customFields as table, tags as table, users as table) as record =>
    let
        customFieldId = current[customFieldId],
        
        // Search whould only retrieve one row.
        customFieldRecord = Table.First(Table.SelectRows(customFields, each [customFieldId] = customFieldId), []),
        fieldName = customFieldRecord[name],
        t = customFieldRecord[type],
        rawFieldValue = if Comparer.OrdinalIgnoreCase(t, "checkbox") = 0 then Logical.ToText(current[value])
            else if Comparer.OrdinalIgnoreCase(t, "date") = 0 then Text.From(current[value])
            else if Comparer.OrdinalIgnoreCase(t, "link") = 0 then Text.From(current[link][url])
            else if Comparer.OrdinalIgnoreCase(t, "members") = 0 then GetUserNames(current[value], users)
            else if Comparer.OrdinalIgnoreCase(t, "number") = 0 then Text.From(current[total]) 
            else if Comparer.OrdinalIgnoreCase(t, "rating") = 0 then Text.From(current[total]) 
            else if Comparer.OrdinalIgnoreCase(t, "single select") = 0 then GetSelected(current[value], customFieldRecord[customFieldItems])
            else if Comparer.OrdinalIgnoreCase(t, "tags") = 0 then GetTagString(current[value], tags)
            else if Comparer.OrdinalIgnoreCase(t, "text") = 0 then Text.From(current[value]) 
            else if Comparer.OrdinalIgnoreCase(t, "time") = 0 then Text.From(current[total])
            else if Comparer.OrdinalIgnoreCase(t, "vote") = 0 then GetUserNames(current[value], users)
            else {"Field type: [" & t & "] Not Found"},
        fieldValue = if (rawFieldValue = null) then "" else rawFieldValue,

        newRecord = if (Record.HasFields(state, fieldName))
            then
                let
                    oldFieldValue = Record.Field(state, fieldName),
                    modRec = Record.RemoveFields(state, fieldName)
                in
                    Record.AddField(modRec, fieldName, oldFieldValue & (if Text.Length(oldFieldValue) > 0 then ", " else "") & fieldValue)
            else Record.AddField(state, fieldName, fieldValue)
    in
        newRecord;

// Find the text value represented id
GetSelected = (itemIds as list, possibleValues as list) as text =>
    let
        itemTable = Table.FromRecords(possibleValues),
        items = List.Accumulate(itemIds, "", (state, current) => state & (if Text.Length(state) = 0 then "" else ", ") & Table.First(Table.SelectRows(itemTable, each[customFieldItemId] = current))[name])
    in
        items;

GetTagString = (tagIds as list, tags as table) as text => 
    List.Accumulate(tagIds, "", (state, current) => state & (if (Text.Length(state) > 0) then ", " else "") & GetTagName(current, tags));

GetUserNames = (userIds as list, users as table) as text =>
    List.Accumulate(userIds, "", (state, current) => state & (if (Text.Length(state) > 0) then ", " else "") & GetUserName(current, users));

GetTagName = (tagId as text, tags as table) as text => Text.From(Table.First(Table.SelectRows(tags, each [tagId] = tagId), [name = ""])[name]);

GetUserName = (userId as text, users as table) as text => Text.From(Table.First(Table.SelectRows(users, each [userId] = userId), [name = ""])[name]);

GetCollectionName = (organizationId as text, collectionId as text) as text =>
    let
        pathUrl = Uri.Combine(BaseUrl, "collections" & "/" & collectionId),
        response = Web.Contents(pathUrl, [ Headers = GetHeader(organizationId) ]),
        body = Json.Document(response)
    in
        body[name];

GetCustomFields = (organizationId as text) as table => GetEntity("customfields", organizationId);

GetTags = (organizationId as text) as table => GetEntity("tags", organizationId);

GetUsers = (organizationId as text) as table => GetEntity("users", organizationId);

GetOrganizationId = () as text =>
    let
        // Gets only one page or results (as there should never be more than that
        result = GetBody(GetHeader(), BaseUrl & "organizations"),
        organizations = result[entities],

        // There should only be one organization being returned in the list of organizations.
        organization = List.First(organizations)
    in
        organization[organizationId];

GetCollectionId = (collectionName as text, organizationId as text) as text =>
    let
        collection = GetEntity("collections", organizationId),
        collectionRows = Table.SelectRows(collection, each Comparer.OrdinalIgnoreCase([name], collectionName) = 0),
        collectionId = if (Table.RowCount(collectionRows) = 0) then "unknown" else Table.First(collectionRows)[collectionId]
    in
        collectionId;

GetWidgets = (organizationId as text, collectionId as text) as table => GetEntity("widgets", organizationId, [collectionId = collectionId]);

// Constructs the header object with the basic authentication
GetHeader = (optional organizationId as text) as record => 
    let
        userId = Extension.CurrentCredential()[Username],
        password = Extension.CurrentCredential()[Password],

        // the a base64 encoded {username}:{password} string from the crede3ntial store
        encodedUserNamePassword = Binary.ToText(Text.ToBinary(userId & ":" & password), BinaryEncoding.Base64),
        organizationaIdlHeader = if (organizationId = null) then DefaultHeaders else Record.AddField(DefaultHeaders, "organizationId", organizationId),
        header = Record.AddField(organizationaIdlHeader, "Authorization", "Basic " & encodedUserNamePassword)
    in
        header;

// Read all pages of data.
// After every page, we check the "NextLink" record on the metadata of the previous request.
// Table.GenerateByPage will keep asking for more pages until we return null.
GetAllPagesByNextLink = (url as text, header as record, optional schema as type) as table =>
    Table.GenerateByPage((previous) => 
        let
            // if previous is null, then this is our first page of data
            nextLink = if (previous = null) then url else
                let
                    queryString = Value.Metadata(previous)[NextLink]?,
                    fullUrl = if (queryString = null) then null else
                        let
                            uriParts = Uri.Parts(url),
                            u1 = if ((uriParts[Query] = null) or (Record.FieldCount(uriParts[Query]) = 0)) then url & "?" & queryString else url & "&" & queryString
                        in
                            u1
                in
                    fullUrl,

            // if NextLink was set to null by the previous call, we know we have no more data
            page = if (nextLink <> null) then GetPage(header, nextLink, Value.Metadata(previous)[XFavroBackendIdentifier]?, schema) else null
        in
            page
    );

// Gets a page of data from the API endpoint
GetPage = (header as record, url as text, optional xFavroBackendIdentifier as text, optional schema as type) as table =>
    let
        // Add X-Favro-Backend-Identifier header to request, if we are paging so that the save server handles the call.
        updatedHeader = if (xFavroBackendIdentifier = null) then header else Record.AddField(header, "X-Favro-Backend-Identifier", xFavroBackendIdentifier),
        response = Web.Contents(url, [ Headers = updatedHeader ]),

        // Get the X-Favro-Backend-Identifier from teh response header for next call, if paging.
        responseHeaders = Value.Metadata(response)[Headers],
        xFavroBackendIdentifierHeaderResponse = responseHeaders[#"X-Favro-Backend-Identifier"],

        body = Json.Document(response),
        nextLink = GetNextLink(body),

        // If we have no schema, use Table.FromRecords() instead
        // (and hope that our results all have the same fields).
        // If we have a schema, expand the record using its field names
        data =
            if (schema = null) then
                Table.FromRecords(body[entities])
            else
                let
                    // convert the list of records into a table (single column of records)
                    asTable = Table.FromList(body[entities], Splitter.SplitByNothing(), {"Column1"}),
                    fields = Record.FieldNames(Type.RecordFields(Type.TableRow(schema))),
                    expanded = Table.ExpandRecordColumn(asTable, "Column1", fields)
                in
                    expanded
    in
        data meta [NextLink = nextLink, XFavroBackendIdentifier = xFavroBackendIdentifierHeaderResponse];

GetBody = (header as record, url as text) =>
    let
        response = Web.Contents(url, [ Headers = header ]),
        body = Json.Document(response)
    in
        body;

// Checks reponse to see if there is a next page. If there is a next page, then return a constructed link for the next page, else return null
GetNextLink = (response) as nullable text =>
    let
        currentPage = Record.FieldOrDefault(response, "page", "0"),     // 0 based
        lastPage = Record.FieldOrDefault(response, "pages", "1"),       // 1 based
        nextPage = currentPage + 1,
        nextLink = if (nextPage < lastPage) then Uri.BuildQueryString([page = Number.ToText(nextPage), requestId = Record.FieldOrDefault(response, "requestId")]) else null
    in
        nextLink;

// Entity Functions
GetEntity = (entity as text, organizationId as text, optional params as record) as table => 
    let
        pathUrl = Uri.Combine(BaseUrl, entity),
        fullUrl = if (params = null) then pathUrl else pathUrl & "?" & Uri.BuildQueryString(params),
        schema = GetSchemaForEntity(entity),
        result = UnityFavro.Feed(fullUrl, organizationId, schema),
        appliedSchema = Table.ChangeType(result, schema)
    in
        result;

// 
// Load common library functions
// 
// TEMPORARY WORKAROUND until we're able to reference other M modules
Extension.LoadFunction = (name as text) =>
    let
        binary = Extension.Contents(name),
        asText = Text.FromBinary(binary)
    in
        Expression.Evaluate(asText, #shared);

Table.ChangeType = Extension.LoadFunction("Table.ChangeType.pqm");
Table.GenerateByPage = Extension.LoadFunction("Table.GenerateByPage.pqm");
Table.ToNavigationTable = Extension.LoadFunction("Table.ToNavigationTable.pqm");
